package com.evidentdb.transactor

import arrow.core.getOrHandle
import com.evidentdb.domain.*
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream

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
        val builder = StreamsBuilder()
        val eventsOrErrors = buildCommandProcessor(
            transactor, builder, commandTopic, eventTopic
        )
        val justEvents = eventsOrErrors.filterNot {_, event -> event is ErrorEnvelope }
        val eventStreams = justEvents.split()
            .branch { _, event -> event is DatabaseEvent }
            .branch { _, event -> event is BatchEvent }
            .defaultBranch()
        buildCatalog(eventStreams["1"]!!, catalogTopic)
        buildIndex(eventStreams["2"]!!, eventsTopic, streamEventIdsTopic, streamsTopic)
        return builder.build()
    }

    private fun buildCommandProcessor(
        transactor: Transactor,
        builder: StreamsBuilder,
        commandTopic: String,
        eventTopic: String
    ): KStream<DatabaseId, EventEnvelope> {
        val commands = builder.stream<DatabaseId, CommandEnvelope>(commandTopic)
        val eventsOrErrors = commands
            .mapValues { databaseId, command ->
                when (command) {
                    is CreateDatabase -> transactor.handleCreateDatabase(command)
                    is DeleteDatabase -> transactor.handleDeleteDatabase(command)
                    is RenameDatabase -> transactor.handleRenameDatabase(command)
                    is TransactBatch -> transactor.handleTransactBatch(command)
                }.getOrHandle { error ->
                    ErrorEnvelope(
                        EventId.randomUUID(),
                        command.id,
                        databaseId,
                        error
                    )
                }
            }
        eventsOrErrors.to(eventTopic)
        return eventsOrErrors!!
    }

    private fun buildCatalog(
        databaseStream: KStream<DatabaseId, EventEnvelope>,
        catalogTopic: String
    ) {
        databaseStream
            .groupByKey()
            ?.aggregate(
                { EmptyDatabase() },
                { databaseId, event, database: Database? ->
                    when (event) {
                        is DatabaseCreated -> event.data.database
                        is DatabaseDeleted -> null
                        is DatabaseRenamed -> DatabaseRecord(databaseId, event.data.newName)
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
            KeyValue(streamKey(evt.databaseId, evt.stream), evt.id)
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