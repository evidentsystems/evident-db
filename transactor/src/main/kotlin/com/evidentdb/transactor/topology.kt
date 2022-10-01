package com.evidentdb.transactor

import arrow.core.getOrHandle
import com.evidentdb.domain.*
import com.evidentdb.kafka.*
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.processor.api.ContextualProcessor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.Stores
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

object TransactorTopology {
    private val LOGGER: Logger = LoggerFactory.getLogger(TransactorTopology::class.java)

    private const val INTERNAL_COMMAND_SOURCE = "INTERNAL_COMMANDS"

    private const val COMMAND_PROCESSOR = "COMMAND_PROCESSOR"
    private const val DATABASE_INDEXER = "DATABASE_INDEXER"
    private const val BATCH_INDEXER = "BATCH_INDEXER"
    private const val STREAM_INDEXER = "STREAM_INDEXER"
    private const val EVENT_INDEXER = "EVENT_INDEXER"

    const val DATABASE_STORE = "DATABASE_STORE"
    const val DATABASE_LOG_STORE = "DATABASE_LOG_STORE"
    const val BATCH_STORE = "BATCH_STORE"
    const val STREAM_STORE = "STREAM_STORE"
    const val EVENT_STORE = "EVENT_STORE"

    private const val INTERNAL_EVENT_SINK = "INTERNAL_EVENTS"

    fun build(
        internalCommandsTopic: String,
        internalEventsTopic: String,
        meterRegistry: MeterRegistry,
    ): Topology {
        LOGGER.info("Building Transactor Topology: $internalCommandsTopic, $internalEventsTopic")
        val topology = Topology()
        val databaseNameSerde = DatabaseNameSerde()

        topology.addSource(
            INTERNAL_COMMAND_SOURCE,
            Serdes.UUID().deserializer(),
            CommandEnvelopeSerde.CommandEnvelopeDeserializer(),
            internalCommandsTopic,
        )

        topology.addProcessor(
            COMMAND_PROCESSOR,
            ProcessorSupplier { CommandProcessor(meterRegistry) },
            INTERNAL_COMMAND_SOURCE,
        )
        topology.addProcessor(
            DATABASE_INDEXER,
            DatabaseIndexer::create,
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
                databaseNameSerde,
                DatabaseSummarySerde(),
            ),
            COMMAND_PROCESSOR,
            DATABASE_INDEXER,
            STREAM_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DATABASE_LOG_STORE),
                Serdes.String(), // DatabaseLogKey
                BatchSummarySerde(),
            ),
            COMMAND_PROCESSOR,
            DATABASE_INDEXER,
            BATCH_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BATCH_STORE),
                Serdes.UUID(), // BatchId
                Serdes.String(), // DatabaseLogKey
            ),
            COMMAND_PROCESSOR,
            BATCH_INDEXER,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STREAM_STORE),
                Serdes.String(), // StreamKey
                listSerde(Serdes.UUID()), // List<EventId>
            ),
            COMMAND_PROCESSOR,
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

        // Event Log
        topology.addSink(
            INTERNAL_EVENT_SINK,
            internalEventsTopic,
            Serdes.UUID().serializer(),
            EventEnvelopeSerde.EventEnvelopeSerializer(),
            DatabaseStreamPartitioner(),
            COMMAND_PROCESSOR,
        )

        return topology
    }

    private class DatabaseStreamPartitioner: StreamPartitioner<UUID, EventEnvelope> {
        override fun partition(topic: String?, key: UUID?, value: EventEnvelope?, numPartitions: Int): Int =
            partitionByDatabase(value!!.database, numPartitions)
    }

    private class CommandProcessor(val meterRegistry: MeterRegistry):
        ContextualProcessor<CommandId, CommandEnvelope, EventId, EventEnvelope>() {
        private var transactor = KafkaStreamsCommandHandler()

        override fun init(context: ProcessorContext<EventId, EventEnvelope>?) {
            super.init(context)
            transactor.init(
                context().getStateStore(DATABASE_STORE),
                context().getStateStore(DATABASE_LOG_STORE),
                context().getStateStore(BATCH_STORE),
            )
        }

        override fun close() {
            super.close()
            this.transactor = KafkaStreamsCommandHandler()
        }

        override fun process(record: Record<CommandId, CommandEnvelope>?): Unit = runBlocking {
            val sample = Timer.start()

            val command = record?.value() ?: throw IllegalStateException()
            LOGGER.info("Processing command: ${command.id}")
            LOGGER.debug("Command data: $command")
            val event = when (command) {
                is CreateDatabase -> transactor.handleCreateDatabase(command)
                is DeleteDatabase -> transactor.handleDeleteDatabase(command)
                is TransactBatch  -> transactor.handleTransactBatch(command)
            }.getOrHandle { error ->
                ErrorEnvelope(
                    EventId.randomUUID(),
                    command.id,
                    command.database,
                    error
                )
            }
            LOGGER.info("Resulting event: ${event.id}")
            LOGGER.debug("Event data: $event")

            context().forward(
                Record(
                    event.id,
                    event,
                    context().currentStreamTimeMs()
                )
            )

            sample.stop(
                meterRegistry.timer(
                    "transactor.processing.duration",
                    "event.type", event.type,
                )
            )
        }
    }

    private class DatabaseIndexer:
        ContextualProcessor<EventId, EventEnvelope, DatabaseName, DatabaseSummary>() {
        lateinit var databaseWriteModel: DatabaseWriteModel

        override fun init(context: ProcessorContext<DatabaseName, DatabaseSummary>?) {
            super.init(context)
            this.databaseWriteModel = DatabaseWriteModel(context().getStateStore(DATABASE_STORE))
        }

        override fun process(record: Record<EventId, EventEnvelope>?) {
            val event = record?.value() ?: throw IllegalStateException()
            val (databaseName, result) = EventHandler.databaseUpdate(event)
            when(result) {
                is DatabaseOperation.StoreDatabase -> databaseWriteModel.putDatabaseSummary(databaseName, result.databaseSummary)
                DatabaseOperation.DeleteDatabase -> databaseWriteModel.deleteDatabase(databaseName)
                DatabaseOperation.DoNothing -> Unit
            }
        }

        companion object {
            fun create(): DatabaseIndexer {
                return DatabaseIndexer()
            }
        }
    }

    private class BatchIndexer:
        ContextualProcessor<EventId, EventEnvelope, BatchKey, BatchSummary>() {
        lateinit var batchSummaryStore: BatchSummaryStore
        lateinit var databaseLogStore: DatabaseLogStore

        override fun init(context: ProcessorContext<BatchKey, BatchSummary>?) {
            super.init(context)
            val databaseLogKeyValueStore: DatabaseLogKeyValueStore = context().getStateStore(DATABASE_LOG_STORE)
            this.batchSummaryStore = BatchSummaryStore(
                context().getStateStore(BATCH_STORE),
                databaseLogKeyValueStore
            )
            this.databaseLogStore = DatabaseLogStore(databaseLogKeyValueStore)
        }

        override fun process(record: Record<EventId, EventEnvelope>?) {
            val event = record?.value() ?: throw IllegalStateException()
            EventHandler.batchToIndex(event)?.let { batchSummary ->
                batchSummaryStore.addKeyLookup(batchSummary)
                databaseLogStore.append(batchSummary)
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
        lateinit var streamStore: StreamSummaryStore

        override fun init(context: ProcessorContext<StreamKey, List<EventId>>?) {
            super.init(context)
            this.streamStore = StreamSummaryStore(
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
                    streamStore.putStreamEventIds(streamKey, eventIds)
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