package com.evidentdb.kafka

import com.evidentdb.domain.*
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.Flow
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
typealias StreamKeyValueStore = KeyValueStore<StreamKey, EventId>
typealias StreamReadOnlyKeyValueStore = ReadOnlyKeyValueStore<StreamKey, EventId>
typealias EventKeyValueStore = KeyValueStore<EventKey, CloudEvent>
typealias EventReadOnlyKeyValueStore = ReadOnlyKeyValueStore<EventKey, CloudEvent>

open class DatabaseLogReadModelStore(
    private val logKeyValueStore: DatabaseLogReadOnlyKeyValueStore,
) {
    fun log(database: DatabaseName): Flow<BatchSummary> = flow {
        logKeyValueStore.range(
            minDatabaseLogKey(database),
            maxDatabaseLogKey(database)
        ).use {
            for (kv in it)
                emit(kv.value.copy())
        }
    }

    fun entry(database: DatabaseName, revision: DatabaseRevision): BatchSummary? =
        logKeyValueStore.get(buildDatabaseLogKey(database, revision))

    fun latest(database: DatabaseName): BatchSummary? =
        logKeyValueStore.reverseRange(
            minDatabaseLogKey(database),
            maxDatabaseLogKey(database)
        ).use {
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
        logKeyValueStore.putIfAbsent(
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

    override fun log(name: DatabaseName): Flow<BatchSummary>? =
        if (exists(name))
            logStore.log(name)
        else
            null

    override fun database(
        name: DatabaseName,
        revision: DatabaseRevision?
    ): Database? =
        databaseStore.get(name)?.let { databaseSummary ->
            val streamRevisions: Map<StreamName, StreamRevision>? =
                if (revision == null)
                    logStore.latest(name)?.streamRevisions
                else
                    // TODO: seek to revision key, rather than exact lookup, to
                    //  tolerate users speculating about revision? Still fail if
                    //  given revision exceeds available revision
                    logStore.entry(name, revision)?.streamRevisions
            Database(
                name,
                databaseSummary.topic,
                databaseSummary.created,
                streamRevisions ?: mapOf()
            )
        }

    override fun catalog(): Flow<Database> = flow {
        databaseStore.all().use { databaseIterator ->
            for (kv in databaseIterator) {
                val databaseSummary = kv.value
                emit(
                    Database(
                        databaseSummary.name,
                        databaseSummary.topic,
                        databaseSummary.created,
                        logStore.latest(databaseSummary.name)
                            ?.streamRevisions
                            ?: mapOf()
                    )
                )
            }
        }
    }
}

class DatabaseWriteModel(
    private val databaseStore: DatabaseKeyValueStore,
) {
    fun putDatabaseSummary(databaseName: DatabaseName, databaseSummary: DatabaseSummary) {
        databaseStore.putIfAbsent(databaseName, databaseSummary)
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
        batchKeyLookup.putIfAbsent(
            batch.id,
            buildDatabaseLogKey(batch.database, batch.revision)
        )
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
        databaseLogStore.range(
            minDatabaseLogKey(database),
            maxDatabaseLogKey(database)
        ).use {
            for (keyValue in it.asSequence()) {
                val batch = keyValue.value
                for (event in batch.events) {
                    eventStore.delete(buildEventKey(
                        database,
                        event.id
                    ))
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
    override fun batch(database: DatabaseName, id: BatchId): Batch? =
        batchKeyLookup.get(id)?.let { databaseLogKey ->
            databaseLogStore.get(databaseLogKey)?.let { summary ->
                Batch(
                    summary.id,
                    summary.database,
                    summary.events.map { summaryEvent ->
                        Event(
                            summary.database,
                            eventStore.get(
                                buildEventKey(
                                    summary.database,
                                    summaryEvent.id,
                                )
                            )!!,
                            summaryEvent.stream
                        )
                    },
                    summary.streamRevisions,
                    summary.timestamp,
                )
            }
        }
}

interface IStreamReadOnlyStore: StreamReadModel {
    val streamStore: StreamReadOnlyKeyValueStore

    override fun stream(
        databaseName: DatabaseName,
        name: StreamName
    ): Flow<EventId> = flow {
        val streamKeyPrefix = buildStreamKeyPrefix(databaseName, name)
        streamStore.prefixScan(
            streamKeyPrefix,
            Serdes.String().serializer(),
        ).use { eventIds ->
            for (entry in eventIds)
                emit(entry.value)
        }
    }

    override fun subjectStream(
        databaseName: DatabaseName,
        name: StreamName,
        subject: EventSubject
    ): Flow<EventId> {
        TODO("Not yet implemented")
    }

    override fun streamState(
        databaseName: DatabaseName,
        name: StreamName
    ): StreamState {
        return streamStore.prefixScan(
            buildStreamKeyPrefix(databaseName, name),
            Serdes.String().serializer()
        ).use { eventIds ->
            if (eventIds.hasNext())
                StreamState.AtRevision(
                    eventIds.asSequence().count().toLong()
                )
            else
                StreamState.NoStream
        }
    }
}

interface IStreamStore: IStreamReadOnlyStore {
    override val streamStore: StreamKeyValueStore

    fun putStreamEventId(streamKey: StreamKey, eventId: EventId) {
        streamStore.putIfAbsent(streamKey, eventId)
    }

    fun deleteStreams(database: DatabaseName) {
        streamStore.prefixScan(
            buildStreamKeyPrefix(database),
            Serdes.String().serializer(),
        ).use { streamEntries ->
            for (entry in streamEntries)
                streamStore.delete(entry.key)
        }
    }
}

class StreamStore(
    override val streamStore: StreamKeyValueStore
): IStreamStore

class StreamReadOnlyStore(
    override val streamStore: StreamReadOnlyKeyValueStore,
): IStreamReadOnlyStore

interface IEventReadOnlyStore: EventReadModel {
    val eventStore: EventReadOnlyKeyValueStore

    override fun event(database: DatabaseName, id: EventId): CloudEvent? =
        eventStore.get(buildEventKey(database, id))
}

interface IEventStore: IEventReadOnlyStore {
    override val eventStore: EventKeyValueStore

    fun putEvent(
        database: DatabaseName,
        eventId: EventId,
        event: CloudEvent
    ) {
        eventStore.putIfAbsent(buildEventKey(database, eventId), event)
    }
}

class EventReadOnlyStore(
    override val eventStore: EventReadOnlyKeyValueStore
): IEventReadOnlyStore

class EventStore(
    override val eventStore: EventKeyValueStore
): IEventStore