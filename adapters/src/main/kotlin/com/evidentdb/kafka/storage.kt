package com.evidentdb.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import com.evidentdb.domain.*
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

typealias DatabaseKeyValueStore = KeyValueStore<DatabaseName, Database>
typealias DatabaseReadOnlyKeyValueStore = ReadOnlyKeyValueStore<DatabaseName, Database>
typealias BatchKeyValueStore = KeyValueStore<BatchKey, List<EventId>>
typealias StreamKeyValueStore = KeyValueStore<StreamKey, List<EventId>>
typealias EventKeyValueStore = KeyValueStore<EventId, Event>

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
                ret.add(kv.value)
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

interface IBatchSummaryStore: BatchSummaryReadModel {
    val batchStore: BatchKeyValueStore

    override fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary? =
        batchStore.get(buildBatchKey(database, id))?.let {eventIds ->
            BatchSummary(
                id,
                database,
                eventIds
            )
        }

    fun putBatchSummary(batch: BatchSummary) {
        batchStore.put(buildBatchKey(batch.database, batch.id), batch.eventIds)
    }
}

class BatchSummaryStore(
    override val batchStore: BatchKeyValueStore
): IBatchSummaryStore

class BatchStore(
    override val batchStore: BatchKeyValueStore,
    private val eventStore: EventKeyValueStore
): IBatchSummaryStore, BatchReadModel {
    override fun batch(database: DatabaseName, id: BatchId): Batch? =
        batchStore.get(buildBatchKey(database, id))?.let {eventIds ->
            Batch(
                id,
                database,
                eventIds.map { eventStore.get(it) }
            )
        }
}

interface IStreamSummaryStore: StreamSummaryReadModel {
    val streamStore: StreamKeyValueStore

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
            databaseName.value,
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
            val (databaseName, streamName) = parseStreamKey(streamKey)
            BaseStreamSummary(
                databaseName,
                streamName,
                eventIds
            )
        }

    // eventIds must be the full list, not just the new ones to append
    fun putStreamEventIds(streamKey: StreamKey, eventIds: List<EventId>) {
        streamStore.put(streamKey, eventIds)
    }
}

class StreamSummaryStore(override val streamStore: StreamKeyValueStore): IStreamSummaryStore

class StreamStore(
    override val streamStore: StreamKeyValueStore,
    private val eventStore: EventKeyValueStore
): IStreamSummaryStore, StreamReadModel {
    override fun stream(databaseName: DatabaseName, name: StreamName): Stream? {
        val eventIds = streamStore.get(buildStreamKey(databaseName, name))
        return if (eventIds == null)
            null
        else
            BaseStream(
                databaseName,
                name,
                eventIds.map { eventStore.get(it)!! }
            )
    }
}

class EventStore(
    private val eventStore: EventKeyValueStore
): EventReadModel {
    override fun event(id: EventId): Event? =
        eventStore.get(id)

    fun putEvent(eventId: EventId, event: Event) {
        eventStore.put(eventId, event)
    }
}
