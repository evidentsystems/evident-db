package com.evidentdb.transactor

import arrow.core.getOrHandle
import com.evidentdb.domain.*
import com.evidentdb.kafka.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.processor.api.*
import org.apache.kafka.streams.state.Stores
import java.util.*

object Topology {
    private const val INTERNAL_COMMAND_SOURCE = "INTERNAL_COMMANDS"

    private const val COMMAND_PROCESSOR = "COMMAND_PROCESSOR"
    private const val DATABASE_INDEXER = "DATABASE_INDEXER"
    private const val DATABASE_NAME_INDEXER = "DATABASE_NAME_INDEXER"
    private const val BATCH_INDEXER = "BATCH_INDEXER"
    private const val STREAM_INDEXER = "STREAM_INDEXER"
    private const val EVENT_INDEXER = "EVENT_INDEXER"

    private const val DATABASE_STORE = "DATABASE_STORE"
    private const val DATABASE_NAME_LOOKUP = "DATABASE_NAME_LOOKUP"
    private const val BATCH_STORE = "BATCH_STORE"
    private const val STREAM_STORE = "STREAM_STORE"
    private const val EVENT_STORE = "EVENT_STORE"

    private const val INTERNAL_EVENT_SINK = "INTERNAL_EVENTS"
    private const val DATABASE_SINK = "DATABASES"
    private const val DATABASE_NAMES_SINK = "DATABASE_NAMES"
    private const val BATCHES_SINK = "BATCHES"
    private const val STREAMS_SINK = "STREAMS"
    private const val EVENTS_SINK = "EVENTS"

    fun build(
        internalCommandTopic: String,
        internalEventsTopic: String,
        databasesTopic: String,
        databaseNamesTopic: String,
        batchesTopic: String,
        streamsTopic: String,
        eventsTopic: String,
    ): Topology {
        val topology = Topology()

        topology.addSource(
            INTERNAL_COMMAND_SOURCE,
            Serdes.UUID().deserializer(),
            CommandEnvelopeSerde.CommandEnvelopeDeserializer(),
            internalCommandTopic,
        )

        topology.addProcessor(
            COMMAND_PROCESSOR,
            CommandProcessor::create,
            INTERNAL_COMMAND_SOURCE,
        )
        topology.addProcessor(
            DATABASE_INDEXER,
            DatabaseIndexer::create,
            COMMAND_PROCESSOR,
        )
        topology.addProcessor(
            DATABASE_NAME_INDEXER,
            DatabaseNameIndexer::create,
            COMMAND_PROCESSOR,
        )
        topology.addProcessor(
            BATCH_INDEXER,
            BatchIndexer::create,
            COMMAND_PROCESSOR,
        )
        topology.addProcessor(
            STREAM_INDEXER,
            StreamIndexer::create,
            COMMAND_PROCESSOR,
        )
        topology.addProcessor(
            EVENT_INDEXER,
            EventIndexer::create,
            COMMAND_PROCESSOR,
        )

        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DATABASE_STORE),
                Serdes.UUID(), // DatabaseId
                DatabaseSerde(),
            ),
            COMMAND_PROCESSOR,
            DATABASE_INDEXER,
            DATABASE_NAME_INDEXER,
            STREAM_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DATABASE_NAME_LOOKUP),
                Serdes.String(), // DatabaseName
                Serdes.UUID(),    // DatabaseId
            ),
            COMMAND_PROCESSOR,
            DATABASE_INDEXER,
            DATABASE_NAME_INDEXER,
            STREAM_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BATCH_STORE),
                Serdes.String(), // BatchKey
                Serdes.ListSerde<EventId>(),
            ),
            BATCH_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STREAM_STORE),
                Serdes.String(), // StreamKey
                Serdes.ListSerde<EventId>(),
            ),
            COMMAND_PROCESSOR,
            STREAM_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(EVENT_STORE),
                Serdes.UUID(), // EventId
                EventSerde(),
            ),
            STREAM_INDEXER,
            EVENT_INDEXER,
        )

        // Event Log
        topology.addSink(
            INTERNAL_EVENT_SINK,
            internalEventsTopic,
            Serdes.UUID().serializer(),
            EventEnvelopeSerde().serializer(),
            DatabaseIdStreamPartitioner(),
            COMMAND_PROCESSOR,
        )
        // Read Model, compacted
        topology.addSink(
            DATABASE_SINK,
            databasesTopic,
            Serdes.UUID().serializer(),
            DatabaseSerde().serializer(),
            // TODO: already partitioned on databaseId, since key
            DATABASE_INDEXER,
        )
        // Read Model, compacted
        topology.addSink(
            DATABASE_NAMES_SINK,
            databaseNamesTopic,
            Serdes.String().serializer(), // DatabaseName
            Serdes.UUID().serializer(),   // DatabaseId
            // TODO: partitioned on database?
            DATABASE_NAME_INDEXER,
        )
        // Read Model, compacted
        topology.addSink(
            BATCHES_SINK,
            batchesTopic,
            Serdes.String().serializer(), // BatchKey
            Serdes.ListSerde<EventId>().serializer(),
            // TODO: partitioned on database?
            BATCH_INDEXER,
        )
        // Read Model, compacted
        topology.addSink(
            STREAMS_SINK,
            streamsTopic,
            Serdes.String().serializer(), // StreamKey
            Serdes.ListSerde<EventId>().serializer(),
            // TODO: partitioned on database?
            STREAM_INDEXER,
        )
        // User-space event log
        topology.addSink(
            EVENTS_SINK,
            eventsTopic,
            Serdes.UUID().serializer(), // EventId
            EventSerde().serializer(),
            // TODO: partitioned on database?
            EVENT_INDEXER,
        )

        return topology
    }

    private class DatabaseIdStreamPartitioner: StreamPartitioner<UUID, EventEnvelope> {
        override fun partition(topic: String?, key: UUID?, value: EventEnvelope?, numPartitions: Int): Int =
            partitionByDatabaseId(value!!.databaseId, numPartitions)
    }

    private class CommandProcessor:
        ContextualProcessor<CommandId, CommandEnvelope, EventId, EventEnvelope>() {
        private var transactor = KafkaStreamsCommandHandler()

        override fun init(context: ProcessorContext<EventId, EventEnvelope>?) {
            super.init(context)
            transactor.init(
                context().getStateStore(DATABASE_STORE),
                context().getStateStore(DATABASE_NAME_LOOKUP),
                context().getStateStore(STREAM_STORE)
            )
        }

        override fun close() {
            super.close()
            this.transactor = KafkaStreamsCommandHandler()
        }

        override fun process(record: Record<CommandId, CommandEnvelope>?) {
            val command = record?.value() ?: throw IllegalStateException()
            val event = when (command) {
                is CreateDatabase -> transactor.handleCreateDatabase(command)
                is DeleteDatabase -> transactor.handleDeleteDatabase(command)
                is RenameDatabase -> transactor.handleRenameDatabase(command)
                is TransactBatch -> transactor.handleTransactBatch(command)
            }.getOrHandle { error ->
                ErrorEnvelope(
                    EventId.randomUUID(),
                    command.id,
                    command.databaseId,
                    error
                )
            }
            context().forward(
                Record(
                    event.id,
                    event,
                    context().currentStreamTimeMs()
                )
            )
        }

        companion object {
            fun create(): CommandProcessor {
                return CommandProcessor()
            }
        }
    }

    private class DatabaseIndexer:
        ContextualProcessor<EventId, EventEnvelope, DatabaseId, Database>() {
        lateinit var databaseStore: DatabaseStore

        override fun init(context: ProcessorContext<DatabaseId, Database>?) {
            super.init(context)
            this.databaseStore = DatabaseStore(
                context().getStateStore(DATABASE_STORE),
                context().getStateStore(DATABASE_NAME_LOOKUP)
            )
        }

        override fun process(record: Record<EventId, EventEnvelope>?) {
            val event = record?.value() ?: throw IllegalStateException()
            val result = EventHandler.databaseUpdate(event)
            if (result != null) {
                val (databaseId, database) = result
                context().forward(
                    Record(
                        databaseId,
                        database,
                        context().currentStreamTimeMs()
                    )
                )
                if (database == null) {
                    databaseStore.deleteDatabase(databaseId)
                } else {
                    databaseStore.putDatabase(databaseId, database)
                }
            }
        }

        companion object {
            fun create(): DatabaseIndexer {
                return DatabaseIndexer()
            }
        }
    }

    private class DatabaseNameIndexer:
        ContextualProcessor<EventId, EventEnvelope, DatabaseName, DatabaseId>() {
        lateinit var databaseStore: DatabaseStore

        override fun init(context: ProcessorContext<DatabaseName, DatabaseId>?) {
            super.init(context)
            this.databaseStore = DatabaseStore(
                context().getStateStore(DATABASE_STORE),
                context().getStateStore(DATABASE_NAME_LOOKUP)
            )
        }

        override fun process(record: Record<EventId, EventEnvelope>?) {
            val event = record?.value() ?: throw IllegalStateException()
            val result = EventHandler.databaseNameLookupUpdate(event)
            if (result != null) {
                val (databaseName, databaseId) = result
                context().forward(
                    Record(
                        databaseName,
                        databaseId,
                        context().currentStreamTimeMs()
                    )
                )
                if (databaseId == null) {
                    databaseStore.deleteDatabaseName(databaseName)
                } else {
                    databaseStore.putDatabaseName(databaseName, databaseId)
                }
            }
        }

        companion object {
            fun create(): DatabaseNameIndexer {
                return DatabaseNameIndexer()
            }
        }
    }

    private class BatchIndexer:
        ContextualProcessor<EventId, EventEnvelope, BatchKey, List<EventId>>() {
        lateinit var batchStore: BatchStore

        override fun init(context: ProcessorContext<BatchKey, List<EventId>>?) {
            super.init(context)
            this.batchStore = BatchStore(
                context().getStateStore(BATCH_STORE)
            )
        }

        override fun process(record: Record<EventId, EventEnvelope>?) {
            val event = record?.value() ?: throw IllegalStateException()
            val result = EventHandler.batchToIndex(event)
            if (result != null) {
                val (batchKey, eventIds) = result
                context().forward(
                    Record(
                        batchKey,
                        eventIds,
                        context().currentStreamTimeMs()
                    )
                )
//                if (eventIds == null) {
//                    streamStore.deleteStream(streamKey)
//                } else {
                batchStore.putBatchEventIds(batchKey, eventIds)
//                }
            }
        }

        companion object {
            fun create(): BatchIndexer {
                return BatchIndexer()
            }
        }
    }

    private class StreamIndexer:
        ContextualProcessor<EventId, EventEnvelope, StreamKey, List<EventId>>() {
        lateinit var streamStore: StreamStore

        override fun init(context: ProcessorContext<StreamKey, List<EventId>>?) {
            super.init(context)
            this.streamStore = StreamStore(
                context().getStateStore(STREAM_STORE)
            )
        }

        override fun process(record: Record<EventId, EventEnvelope>?) {
            val event = record?.value() ?: throw IllegalStateException()
            val result = EventHandler.streamEventIdsToUpdate(
                streamStore,
                event
            )
            if (result != null) {
                for ((streamKey, eventIds) in result) {
                    context().forward(
                        Record(
                            streamKey,
                            eventIds,
                            context().currentStreamTimeMs()
                        )
                    )
//                if (eventIds == null) {
//                    streamStore.deleteStream(streamKey)
//                } else {
                    streamStore.putStreamEventIds(streamKey, eventIds)
//                }
                }
            }
        }

        companion object {
            fun create(): StreamIndexer {
                return StreamIndexer()
            }
        }
    }

    private class EventIndexer:
        ContextualProcessor<EventId, EventEnvelope, EventId, Event>() {
        lateinit var eventStore: EventStore

        override fun init(context: ProcessorContext<EventId, Event>?) {
            super.init(context)
            this.eventStore = EventStore(
                context().getStateStore(EVENT_STORE)
            )
        }

        override fun process(record: Record<EventId, EventEnvelope>?) {
            val internalEvent = record?.value() ?: throw IllegalStateException()
            val result = EventHandler.eventsToIndex(internalEvent)
            if (result != null) {
                for ((eventId, event) in result) {
                    context().forward(
                        Record(
                            eventId,
                            event,
                            context().currentStreamTimeMs()
                        )
                    )
                    eventStore.putEvent(eventId, event)
                }
            }
        }

        companion object {
            fun create(): EventIndexer {
                return EventIndexer()
            }
        }
    }
}