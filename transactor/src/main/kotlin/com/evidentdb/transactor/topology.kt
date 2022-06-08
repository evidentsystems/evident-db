package com.evidentdb.transactor

import arrow.core.getOrHandle
import com.evidentdb.domain.*
import com.evidentdb.kafka.partitionBySource
import io.cloudevents.CloudEvent
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.processor.api.*
import java.util.*

class EventSourceStreamPartitioner: StreamPartitioner<UUID, CloudEvent> {
    override fun partition(topic: String?, key: UUID?, value: CloudEvent?, numPartitions: Int): Int =
        partitionBySource(value!!, numPartitions)
}

class CommandProcessor:
    ContextualProcessor<CommandId, CommandEnvelope, EventId, EventEnvelope>() {
    private var transactor: Transactor = KafkaStreamsTransactor()

    override fun init(context: ProcessorContext<EventId, EventEnvelope>?) {
        super.init(context)
        // transactor.init() w/ state stores
    }

    override fun close() {
        super.close()
        this.transactor = KafkaStreamsTransactor()
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
        TODO()
    }

    companion object {
        fun create(): CommandProcessor {
            return CommandProcessor()
        }
    }
}

object Topology {
    fun build(
        transactor: Transactor,
        commandTopic: String,
        eventTopic: String,
        catalogTopic: String,
        eventsTopic: String,
        streamEventIdsTopic: String,
        streamsTopic: String
    ): Topology {
        val topology = Topology()
        topology.addSource("commands", commandTopic)
        topology.addProcessor(
            "commandProcessor",
            CommandProcessor::create,
            "commands"
        )

        TODO()
//        val justEvents = eventsOrErrors.filterNot {_, event -> event is ErrorEnvelope }
//        val eventStreams = justEvents.split()
//            .branch { _, event -> event is DatabaseEvent }
//            .branch { _, event -> event is BatchEvent }
//            .defaultBranch()
//        buildCatalog(eventStreams["1"]!!, catalogTopic)
//        buildIndex(eventStreams["2"]!!, eventsTopic, streamEventIdsTopic, streamsTopic)
//        return topology
    }

    private fun buildCommandProcessor(
        transactor: Transactor,
        topology: Topology,
        commandTopic: String,
        eventTopic: String
    ) {
    }

    private fun buildCatalog(
        databaseStream: KStream<DatabaseId, EventEnvelope>,
        catalogTopic: String
    ) {
        databaseStream
            .groupByKey()
            ?.aggregate(
                { EmptyDatabase() },
                { databaseId, event, database: IDatabase? ->
                    when (event) {
                        is DatabaseCreated -> event.data.database
                        is DatabaseDeleted -> null
                        is DatabaseRenamed -> Database(databaseId, event.data.newName)
                        else -> database
                    }
                }
            )
            ?.toStream()
            ?.to(catalogTopic)
    }

    private fun buildIndex(
        batchStream: KStream<DatabaseId, EventEnvelope>,
        eventsTopic: String,
        streamEventIdsTopic: String,
        streamsTopic: String,
    ) {
        val eventStream = batchStream.flatMap {
                _, event ->
            when(event) {
                is BatchTransacted -> event.data.events.map { evt ->
                    KeyValue(evt.id, evt)
                }
                else -> listOf()
            }
        }
        eventStream.to(eventsTopic)
        val streamEventIdsStream = eventStream.map { _, evt ->
            KeyValue(buildStreamKey(evt.databaseId, evt.stream), evt.id)
        }
        streamEventIdsStream.to(streamEventIdsTopic)

        streamEventIdsStream
            ?.groupByKey()
            ?.aggregate(
                { mutableListOf() },
                { _, evtId, evtIds: MutableList<EventId> ->
                    evtIds.add(evtId)
                    evtIds
                }
            )
            ?.toStream()
            ?.to(streamsTopic)
    }
}