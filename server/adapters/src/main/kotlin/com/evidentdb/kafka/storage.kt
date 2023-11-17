package com.evidentdb.kafka

import com.evidentdb.application.*
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import com.evidentdb.event_model.*
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import java.util.NoSuchElementException

typealias DatabaseKeyValueStore = KeyValueStore<DatabaseName, DatabaseSummary>
typealias DatabaseReadOnlyKeyValueStore = ReadOnlyKeyValueStore<DatabaseName, DatabaseSummary>
typealias DatabaseLogKeyValueStore = KeyValueStore<DatabaseLogKey, LogBatch>
typealias DatabaseLogReadOnlyKeyValueStore = ReadOnlyKeyValueStore<DatabaseLogKey, LogBatch>
typealias BatchIndexKeyValueStore = KeyValueStore<BatchId, DatabaseLogKey>
typealias BatchIndexReadOnlyKeyValueStore = ReadOnlyKeyValueStore<BatchId, DatabaseLogKey>
typealias StreamKeyValueStore = KeyValueStore<StreamKey, EventIndex>
typealias StreamReadOnlyKeyValueStore = ReadOnlyKeyValueStore<StreamKey, EventIndex>
typealias EventKeyValueStore = KeyValueStore<EventKey, CloudEvent>
typealias EventReadOnlyKeyValueStore = ReadOnlyKeyValueStore<EventKey, CloudEvent>

open class DatabaseLogReadModelStore(
    private val logKeyValueStore: DatabaseLogReadOnlyKeyValueStore,
) {
    fun log(database: DatabaseName): Flow<LogBatch> = flow {
        logKeyValueStore.range(
            minDatabaseLogKey(database),
            maxDatabaseLogKey(database)
        ).use {
            for (kv in it)
                emit(kv.value.copy())
        }
    }

    fun entry(database: DatabaseName, revision: DatabaseRevision): LogBatch? =
        logKeyValueStore.get(buildDatabaseLogKey(database, revision))

    fun latest(database: DatabaseName): LogBatch? =
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
    fun append(batch: LogBatch) {
        logKeyValueStore.putIfAbsent(
            buildDatabaseLogKey(batch.database, batch.revision),
            batch,
        )
    }
}

open class DatabaseRepositoryStore(
    private val databaseStore: DatabaseReadOnlyKeyValueStore,
    logKeyValueStore: DatabaseLogReadOnlyKeyValueStore
): DatabaseRepository {
    private val logStore = DatabaseLogReadModelStore(logKeyValueStore)

    override fun log(name: DatabaseName): Flow<LogBatch>? =
        if (exists(name))
            logStore.log(name)
        else
            null

    override fun database(
        name: DatabaseName,
        revision: DatabaseRevision?
    ): ActiveDatabaseCommandModel? =
        databaseStore.get(name)?.let { databaseSummary ->
            val streamRevisions: Map<StreamName, StreamRevision>? =
                if (revision == null)
                    logStore.latest(name)?.streamRevisions
                else
                    // TODO: seek to revision key, rather than exact lookup, to
                    //  tolerate users speculating about revision? Still fail if
                    //  given revision exceeds available revision
                    logStore.entry(name, revision)?.streamRevisions
            ActiveDatabaseCommandModel(
                name,
                databaseSummary.topic,
                databaseSummary.created,
                streamRevisions ?: mapOf()
            )
        }

    override fun catalog(): Flow<ActiveDatabaseCommandModel> = flow {
        databaseStore.all().use { databaseIterator ->
            for (kv in databaseIterator) {
                val databaseSummary = kv.value
                emit(
                    ActiveDatabaseCommandModel(
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

interface IBatchSummaryReadOnlyStore: BatchSummaryRepository {
    val batchKeyLookup: BatchIndexReadOnlyKeyValueStore
    val databaseLogStore: DatabaseLogReadOnlyKeyValueStore

    override fun batchSummary(database: DatabaseName, id: BatchId): LogBatch? =
        batchKeyLookup.get(id)?.let {
            databaseLogStore.get(it)
        }
}

interface IBatchSummaryStore: IBatchSummaryReadOnlyStore {
    override val batchKeyLookup: BatchIndexKeyValueStore

    fun addKeyLookup(batch: LogBatch) {
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
                    eventStore.delete(
                        buildEventKey(
                        database,
                        event.id
                    )
                    )
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
): IBatchSummaryReadOnlyStore, BatchRepository {
    override fun batch(database: DatabaseName, id: BatchId): Batch? =
        batchKeyLookup.get(id)?.let { databaseLogKey ->
            databaseLogStore.get(databaseLogKey)?.let { summary ->
                Batch(
                    summary.id,
                    summary.database,
                    summary.events.map { summaryEvent ->
                        Event(
                            summary.database,
                            summaryEvent.stream,
                            eventStore.get(
                                buildEventKey(
                                    summary.database,
                                    summaryEvent.id,
                                )
                            )!!
                        )
                    },
                    summary.streamRevisions,
                    summary.timestamp,
                )
            }
        }
}

interface IStreamReadOnlyStore: StreamRepository {
    val streamStore: StreamReadOnlyKeyValueStore

    override fun stream(
        databaseName: DatabaseName,
        stream: StreamName
    ): Flow<Pair<StreamRevision, EventIndex>> = flow {
        forEachStreamEntryByPrefix(
            BaseStreamKey(databaseName, stream).streamPrefix(),
            ::emit
        )
    }

    override fun subjectStream(
        databaseName: DatabaseName,
        stream: StreamName,
        subject: EventSubject
    ): Flow<Pair<StreamRevision, EventIndex>> = flow {
        forEachStreamEntryByPrefix(
            SubjectStreamKey(
                databaseName,
                stream,
                subject
            ).streamPrefix(),
            ::emit
        )
    }

    private suspend fun forEachStreamEntryByPrefix(
        streamKeyPrefix: String,
        process: suspend (Pair<StreamRevision, EventIndex>) -> Unit
    ) = streamStore.prefixScan(
        streamKeyPrefix,
        Serdes.String().serializer(),
    ).use { eventIds ->
        for (entry in eventIds)
            process(entry.key.revision!! to entry.value)
    }

    override fun streamState(
        databaseName: DatabaseName,
        stream: StreamName
    ): StreamState =
        // TODO: reverse scan/range, pull revision
        //  off latest key rather than counting
        streamStore.prefixScan(
            BaseStreamKey(databaseName, stream).streamPrefix(),
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

interface IStreamStore: IStreamReadOnlyStore {
    override val streamStore: StreamKeyValueStore

    fun putStreamEventId(streamKey: StreamKey, eventIndex: EventIndex) {
        streamStore.putIfAbsent(streamKey, eventIndex)
    }

    fun deleteStreams(database: DatabaseName) {
        streamStore.prefixScan(
            database.asStreamKeyPrefix(),
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

interface IEventReadOnlyStore: EventRepository {
    val eventStore: EventReadOnlyKeyValueStore

    override fun eventByIndex(database: DatabaseName, index: EventIndex): CloudEvent? =
        eventStore.get(buildEventKey(database, index))
}

interface IEventStore: IEventReadOnlyStore {
    override val eventStore: EventKeyValueStore

    fun putEvent(
        database: DatabaseName,
        eventIndex: EventIndex,
        event: CloudEvent
    ) {
        eventStore.putIfAbsent(buildEventKey(database, eventIndex), event)
    }
}

class EventReadOnlyStore(
    override val eventStore: EventReadOnlyKeyValueStore
): IEventReadOnlyStore

class EventStore(
    override val eventStore: EventKeyValueStore
): IEventStore