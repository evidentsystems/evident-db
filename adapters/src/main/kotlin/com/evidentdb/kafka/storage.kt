package com.evidentdb.kafka

import com.evidentdb.domain.*
import io.cloudevents.CloudEvent
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import java.util.NoSuchElementException

typealias DatabaseKeyValueStore = KeyValueStore<DatabaseName, DatabaseSummary>
typealias DatabaseReadOnlyKeyValueStore = ReadOnlyKeyValueStore<DatabaseName, DatabaseSummary>
typealias DatabaseLogKeyValueStore = KeyValueStore<DatabaseLogKey, BatchSummary>
typealias DatabaseLogReadOnlyKeyValueStore = ReadOnlyKeyValueStore<DatabaseLogKey, BatchSummary>
typealias BatchIndexKeyValueStore = KeyValueStore<BatchId, DatabaseLogKey>
typealias BatchIndexReadOnlyKeyValueStore = ReadOnlyKeyValueStore<BatchId, DatabaseLogKey>
typealias StreamKeyValueStore = KeyValueStore<StreamKey, List<EventId>>
typealias StreamReadOnlyKeyValueStore = ReadOnlyKeyValueStore<StreamKey, List<EventId>>
typealias EventKeyValueStore = KeyValueStore<EventId, CloudEvent>
typealias EventReadOnlyKeyValueStore = ReadOnlyKeyValueStore<EventId, CloudEvent>

open class DatabaseLogReadModelStore(
    private val logKeyValueStore: DatabaseLogReadOnlyKeyValueStore,
) {
    fun log(database: DatabaseName): List<BatchSummary> {
        val ret = mutableListOf<BatchSummary>()
        logKeyValueStore.range(minDatabaseLogKey(database), maxDatabaseLogKey(database)).use {
            for(kv in it) {
                ret.add(kv.value.copy())
            }
        }
        return ret
    }

    fun entry(database: DatabaseName, revision: DatabaseRevision): BatchSummary? =
        logKeyValueStore.get(buildDatabaseLogKey(database, revision))

    fun latest(database: DatabaseName): BatchSummary? =
        logKeyValueStore.reverseRange(minDatabaseLogKey(database), maxDatabaseLogKey(database)).use {
            try {
                it.next().value
            } catch (_: NoSuchElementException) {
                null
            }
        }
}

class DatabaseLogStore(
    private val logKeyValueStore: DatabaseLogKeyValueStore,
): DatabaseLogReadModelStore(logKeyValueStore) {
    fun append(batch: BatchSummary) {
        logKeyValueStore.put(
            buildDatabaseLogKey(batch.database, batch.revision),
            batch,
        )
    }
}

open class DatabaseReadModelStore(
    private val databaseStore: DatabaseReadOnlyKeyValueStore,
    logKeyValueStore: DatabaseLogReadOnlyKeyValueStore
): DatabaseReadModel {
    private val logStore = DatabaseLogReadModelStore(logKeyValueStore)

    override fun log(name: DatabaseName): List<BatchSummary>? =
        if (this.exists(name))
            logStore.log(name)
        else
            null

    override fun database(name: DatabaseName): Database? =
        databaseStore.get(name)?.let {
            logStore.latest(name)?.let {batchSummary ->
                Database(
                    name,
                    it.created,
                    batchSummary.streamRevisions
                )
            } ?: Database(name, it.created, mapOf())
        }

    // TODO: seek to revision key, rather than exact lookup, to tolerate users speculating about revision
    override fun database(
        name: DatabaseName,
        revision: DatabaseRevision
    ): Database? =
        databaseStore.get(name)?.let { databaseSummary ->
            logStore.entry(name, revision)?.let { batchSummary ->
                Database(
                    name,
                    databaseSummary.created,
                    batchSummary.streamRevisions
                )
            }
        }

    override fun summary(name: DatabaseName): DatabaseSummary? =
        databaseStore.get(name)

    override fun catalog(): Set<DatabaseSummary> {
        val ret = mutableSetOf<DatabaseSummary>()
        databaseStore.all().use { databaseIterator ->
            for (kv in databaseIterator)
                ret.add(kv.value.copy())
        }
        return ret
    }
}

class DatabaseWriteModel(
    private val databaseStore: DatabaseKeyValueStore,
) {
    fun putDatabaseSummary(databaseName: DatabaseName, databaseSummary: DatabaseSummary) {
        databaseStore.put(databaseName, databaseSummary)
    }

    fun deleteDatabase(databaseName: DatabaseName) {
        databaseStore.delete(databaseName)
    }
}

interface IBatchSummaryReadOnlyStore: BatchSummaryReadModel {
    val batchKeyLookup: BatchIndexReadOnlyKeyValueStore
    val databaseLogStore: DatabaseLogReadOnlyKeyValueStore

    override fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary? =
        batchKeyLookup.get(id)?.let {
            databaseLogStore.get(it)
        }
}

interface IBatchSummaryStore: IBatchSummaryReadOnlyStore {
    override val batchKeyLookup: BatchIndexKeyValueStore

    fun addKeyLookup(batch: BatchSummary) {
        batchKeyLookup.put(batch.id, buildDatabaseLogKey(batch.database, batch.revision))
    }
}

class BatchSummaryReadOnlyStore(
    override val batchKeyLookup: BatchIndexReadOnlyKeyValueStore,
    override val databaseLogStore: DatabaseLogReadOnlyKeyValueStore,
): IBatchSummaryReadOnlyStore

class BatchSummaryStore(
    override val batchKeyLookup: BatchIndexKeyValueStore,
    override val databaseLogStore: DatabaseLogKeyValueStore,
    private val eventStore: EventKeyValueStore,
): IBatchSummaryStore {
    fun deleteDatabaseLog(database: DatabaseName) {
        databaseLogStore.range(minDatabaseLogKey(database), maxDatabaseLogKey(database))
            .use {
                for (keyValue in it.asSequence()) {
                    val batch = keyValue.value
                    for (event in batch.events) {
                        eventStore.delete(event.id)
                    }
                    batchKeyLookup.delete(batch.id)
                    databaseLogStore.delete(keyValue.key)
                }
            }
    }
}

class BatchReadOnlyStore(
    override val batchKeyLookup: BatchIndexReadOnlyKeyValueStore,
    override val databaseLogStore: DatabaseLogReadOnlyKeyValueStore,
    private val eventStore: EventReadOnlyKeyValueStore,
): IBatchSummaryReadOnlyStore, BatchReadModel {
    override fun batch(id: BatchId): Batch? =
        batchKeyLookup.get(id)?.let { databaseLogKey ->
            databaseLogStore.get(databaseLogKey)?.let { summary ->
                Batch(
                    summary.id,
                    summary.database,
                    summary.events.map { summaryEvent ->
                        Event(
                            summary.database,
                            eventStore.get(summaryEvent.id)!!,
                            summaryEvent.stream
                        )
                    },
                    summary.streamRevisions
                )
            }
        }
}

interface IStreamSummaryReadOnlyStore: StreamSummaryReadModel {
    val streamStore: StreamReadOnlyKeyValueStore

    override fun streamState(databaseName: DatabaseName, name: StreamName): StreamState {
        val eventIds = streamStore.get(buildStreamKey(databaseName, name))
        return if (eventIds == null)
            StreamState.NoStream
        else
            StreamState.AtRevision(eventIds.count().toLong())
    }

    override fun databaseStreams(databaseName: DatabaseName): Set<StreamSummary> {
        val ret = mutableSetOf<StreamSummary>()
        streamStore.prefixScan(
            buildStreamKeyPrefix(databaseName),
            Serdes.String().serializer()
        ).use { streamIterator ->
            for (kv in streamIterator) {
                ret.add(
                    StreamSummary.create(
                        kv.key,
                        kv.value
                    )
                )
            }
        }
        return ret
    }

    override fun streamSummary(streamKey: StreamKey): StreamSummary? =
        streamStore.get(streamKey)?.let { eventIds ->
            StreamSummary.create(streamKey, eventIds)
        }
}

interface IStreamSummaryStore: IStreamSummaryReadOnlyStore {
    override val streamStore: StreamKeyValueStore

    // eventIds must be the full list, not just the new ones to append
    fun putStreamEventIds(streamKey: StreamKey, eventIds: List<EventId>) {
        streamStore.put(streamKey, eventIds)
    }

    fun deleteStream(streamKey: StreamKey) {
        streamStore.delete(streamKey)
    }
}

class StreamSummaryStore(override val streamStore: StreamKeyValueStore): IStreamSummaryStore

class StreamReadOnlyStore(
    override val streamStore: StreamReadOnlyKeyValueStore,
    private val eventStore: EventReadOnlyKeyValueStore
): IStreamSummaryReadOnlyStore, StreamReadModel {
    override fun stream(databaseName: DatabaseName, name: StreamName): Stream? {
        val streamKey = buildStreamKey(databaseName, name)
        return streamStore.get(streamKey)?.let { eventIds ->
            Stream.create(
                streamKey,
                eventIds.map {
                    Event(
                        databaseName,
                        eventStore.get(it)!!,
                        name,
                    )
                }
            )
        }
    }
}

interface IEventReadOnlyStore: EventReadModel {
    val eventStore: EventReadOnlyKeyValueStore

    override fun event(id: EventId): CloudEvent? =
        eventStore.get(id)
}

interface IEventStore: IEventReadOnlyStore {
    override val eventStore: EventKeyValueStore

    fun putEvent(eventId: EventId, event: CloudEvent) {
        eventStore.put(eventId, event)
    }
}

class EventReadOnlyStore(
    override val eventStore: EventReadOnlyKeyValueStore
): IEventReadOnlyStore

class EventStore(
    override val eventStore: EventKeyValueStore
): IEventStore