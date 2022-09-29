package com.evidentdb.kafka

import com.evidentdb.domain.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import java.util.logging.Level
import java.util.logging.Logger

typealias DatabaseKeyValueStore = KeyValueStore<DatabaseName, Database>
typealias DatabaseReadOnlyKeyValueStore = ReadOnlyKeyValueStore<DatabaseName, Database>
typealias BatchKeyValueStore = KeyValueStore<BatchKey, List<EventId>>
typealias BatchReadOnlyKeyValueStore = ReadOnlyKeyValueStore<BatchKey, List<EventId>>
typealias StreamKeyValueStore = KeyValueStore<StreamKey, List<EventId>>
typealias StreamReadOnlyKeyValueStore = ReadOnlyKeyValueStore<StreamKey, List<EventId>>
typealias EventKeyValueStore = KeyValueStore<EventId, Event>
typealias EventReadOnlyKeyValueStore = ReadOnlyKeyValueStore<EventId, Event>

// TODO: Abstract away DatabaseNameLookup into an interface, implement via gRPC
open class DatabaseReadModelStore(
    private val databaseLookupStore: DatabaseReadOnlyKeyValueStore
): DatabaseReadModel {
    override fun database(name: DatabaseName): Database? =
        databaseLookupStore.get(name)

    override fun catalog(): Set<Database> {
        val ret = mutableSetOf<Database>()
        databaseLookupStore.all().use { databaseIterator ->
            for (kv in databaseIterator)
                ret.add(kv.value.copy())
        }
        return ret
    }
}

class DatabaseStore(
    private val databaseStore: DatabaseKeyValueStore,
): DatabaseReadModelStore(databaseStore) {
    fun putDatabase(databaseName: DatabaseName, database: Database) {
        databaseStore.put(databaseName, database)
    }

    fun deleteDatabase(databaseName: DatabaseName) {
        databaseStore.delete(databaseName)
    }
}

interface IBatchSummaryReadOnlyStore: BatchSummaryReadModel {
    val batchStore: BatchReadOnlyKeyValueStore

    override fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary? =
        batchStore.get(buildBatchKey(database, id))?.let {eventIds ->
            BatchSummary(
                id,
                database,
                eventIds
            )
        }
}

interface IBatchSummaryStore: IBatchSummaryReadOnlyStore {
    override val batchStore: BatchKeyValueStore

    fun putBatchSummary(batch: BatchSummary) {
        batchStore.put(buildBatchKey(batch.database, batch.id), batch.eventIds)
    }
}

class BatchSummaryReadOnlyStore(
    override val batchStore: BatchReadOnlyKeyValueStore
): IBatchSummaryReadOnlyStore

class BatchSummaryStore(
    override val batchStore: BatchKeyValueStore
): IBatchSummaryStore

class BatchReadOnlyStore(
    override val batchStore: BatchReadOnlyKeyValueStore,
    private val eventStore: EventReadOnlyKeyValueStore
): IBatchSummaryReadOnlyStore, BatchReadModel {
    override fun batch(database: DatabaseName, id: BatchId): Batch? =
        batchStore.get(buildBatchKey(database, id))?.let { eventIds ->
            Batch(
                id,
                database,
                eventIds.map { eventStore.get(it)!! }
            )
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
                eventIds.map { eventStore.get(it)!! }
            )
        }
    }
}

interface IEventReadOnlyStore: EventReadModel {
    val eventStore: EventReadOnlyKeyValueStore

    override fun event(id: EventId): Event? =
        eventStore.get(id)
}

interface IEventStore: IEventReadOnlyStore {
    override val eventStore: EventKeyValueStore

    fun putEvent(eventId: EventId, event: Event) {
        eventStore.put(eventId, event)
    }
}

class EventReadOnlyStore(
    override val eventStore: EventReadOnlyKeyValueStore
): IEventReadOnlyStore

class EventStore(
    override val eventStore: EventKeyValueStore
): IEventStore