package com.evidentdb.transactor

import com.evidentdb.domain.*
import com.evidentdb.kafka.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.api.*
import org.apache.kafka.streams.state.Stores
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object TransactorTopology {
    private val LOGGER: Logger = LoggerFactory.getLogger(TransactorTopology::class.java)

    private const val LOG_SOURCE = "INTERNAL_EVENT_LOG_SOURCE"

    private const val DATABASE_INDEXER = "DATABASE_INDEXER"
    private const val BATCH_INDEXER = "BATCH_INDEXER"
    private const val STREAM_INDEXER = "STREAM_INDEXER"
    private const val EVENT_INDEXER = "EVENT_INDEXER"

    const val DATABASE_STORE = "DATABASE_STORE"
    const val BATCH_STORE = "BATCH_STORE"
    const val STREAM_STORE = "STREAM_STORE"
    const val EVENT_STORE = "EVENT_STORE"

    fun build(logTopic: String): Topology {
        LOGGER.info("Building Transactor Topology: $logTopic")
        val topology = Topology()
        val databaseNameSerde = DatabaseNameSerde()

        topology.addSource(
            LOG_SOURCE,
            databaseNameSerde.deserializer(),
            EventEnvelopeSerde.EventEnvelopeDeserializer(),
            logTopic,
        )

        topology.addProcessor(
            DATABASE_INDEXER,
            DatabaseIndexer::create,
            LOG_SOURCE,
        )
        topology.addProcessor(
            BATCH_INDEXER,
            BatchIndexer::create,
            LOG_SOURCE,
        )
        topology.addProcessor(
            STREAM_INDEXER,
            StreamIndexer::create,
            LOG_SOURCE,
        )
        topology.addProcessor(
            EVENT_INDEXER,
            EventIndexer::create,
            LOG_SOURCE,
        )

        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DATABASE_STORE),
                databaseNameSerde,
                DatabaseSerde(),
            ),
            DATABASE_INDEXER,
            STREAM_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BATCH_STORE),
                Serdes.String(), // BatchKey
                listSerde(Serdes.UUID()), // List<EventId>
            ),
            BATCH_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STREAM_STORE),
                Serdes.String(), // StreamKey
                listSerde(Serdes.UUID()), // List<EventId>
            ),
            STREAM_INDEXER,
        )

        // Configure structured event serialization for storage (i.e. no headers)
        val eventStoreSerde = EventSerde()
        eventStoreSerde.configure(EvidentDbSerializer.structuredConfig(), false)

        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(EVENT_STORE),
                Serdes.UUID(), // EventId
                eventStoreSerde,
            ),
            STREAM_INDEXER,
            EVENT_INDEXER,
        )

        return topology
    }

    // private class CommandProcessor(val meterRegistry: MeterRegistry):
    //     ContextualProcessor<CommandId, CommandEnvelope, EventId, EventEnvelope>() {
    //     private var transactor = KafkaStreamsCommandHandler()

    //     override fun init(context: ProcessorContext<EventId, EventEnvelope>?) {
    //         super.init(context)
    //         transactor.init(
    //             context().getStateStore(DATABASE_STORE),
    //             context().getStateStore(STREAM_STORE)
    //         )
    //     }

    //     override fun close() {
    //         super.close()
    //         this.transactor = KafkaStreamsCommandHandler()
    //     }

    //     override fun process(record: Record<CommandId, CommandEnvelope>?): Unit = runBlocking {
    //         val sample = Timer.start()

    //         val command = record?.value() ?: throw IllegalStateException()
    //         LOGGER.info("Processing command: ${command.id}")
    //         LOGGER.debug("Command data: $command")
    //         val event = when (command) {
    //             is CreateDatabase -> transactor.handleCreateDatabase(command)
    //             is DeleteDatabase -> transactor.handleDeleteDatabase(command)
    //             is TransactBatch  -> transactor.handleTransactBatch(command)
    //         }.getOrHandle { error ->
    //             ErrorEnvelope(
    //                 EventId.randomUUID(),
    //                 command.id,
    //                 command.database,
    //                 error
    //             )
    //         }
    //         LOGGER.info("Resulting event: ${event.id}")
    //         LOGGER.debug("Event data: $event")

    //         context().forward(
    //             Record(
    //                 event.id,
    //                 event,
    //                 context().currentStreamTimeMs()
    //             )
    //         )

    //         sample.stop(
    //             meterRegistry.timer(
    //                 "transactor.processing.duration",
    //                 "event.type", event.type,
    //             )
    //         )
    //     }
    // }

    private class DatabaseIndexer:
        ContextualProcessor<DatabaseName, EventEnvelope, DatabaseName, Database>() {
        lateinit var databaseStore: DatabaseStore

        override fun init(context: ProcessorContext<DatabaseName, Database>?) {
            super.init(context)
            this.databaseStore = DatabaseStore(
                context().getStateStore(DATABASE_STORE)
            )
        }

        override fun process(record: Record<DatabaseName, EventEnvelope>?) {
            val event = record?.value() ?: throw IllegalStateException()
            val initialDatabase = databaseStore.database(event.database)
            val result = EventHandler.databaseUpdate(initialDatabase, event)
            if (result != null) {
                val (databaseName, database) = result
                if (database == null) {
                    databaseStore.deleteDatabase(databaseName)
                } else {
                    databaseStore.putDatabase(databaseName, database)
                }
            }
        }

        companion object {
            fun create(): DatabaseIndexer {
                return DatabaseIndexer()
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