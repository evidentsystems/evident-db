package com.evidentdb.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import com.evidentdb.domain.*
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

typealias DatabaseKeyValueStore = KeyValueStore<DatabaseName, Database>
typealias DatabaseReadOnlyKeyValueStore = ReadOnlyKeyValueStore<DatabaseName, Database>
typealias BatchKeyValueStore = KeyValueStore<BatchId, BatchSummary>
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

class BatchStore(
    private val batchStore: BatchKeyValueStore
): BatchReadModel {
    override fun batch(id: BatchId): Batch? {
        TODO("Not yet implemented")
    }

    override fun batchSummary(id: BatchId): BatchSummary? =
        batchStore.get(id)

    fun putBatchSummary(batchId: BatchId, batch: BatchSummary) {
        batchStore.put(batchId, batch)
    }
}

interface IStreamStore: StreamReadModel {
    val streamStore: StreamKeyValueStore

    override fun streamState(databaseName: DatabaseName, name: StreamName): StreamState {
        val eventIds = streamStore.get(buildStreamKey(databaseName, name))
        return if (eventIds == null)
            StreamState.NoStream
        else
            StreamState.AtRevision(eventIds.count().toLong())
    }

    override fun stream(databaseName: DatabaseName, name: StreamName): Stream? {
        val eventIds = streamStore.get(buildStreamKey(databaseName, name))
        return if (eventIds == null)
            null
        else
            Stream.create(databaseName, name, eventIds.count().toLong())
    }

    override fun databaseStreams(databaseName: DatabaseName): Set<Stream> {
        val ret = mutableSetOf<Stream>()
        streamStore.prefixScan(
            databaseName.value,
            Serdes.String().serializer()
        ).use { streamIterator ->
            for (kv in streamIterator) {
                val parsedKey = parseStreamKey(kv.key)
                ret.add(
                    Stream.create(
                        parsedKey.first,
                        parsedKey.second,
                        kv.value.count().toLong()
                    )
                )
            }
        }
        return ret
    }

    override fun streamEventIds(streamKey: StreamKey)
            : List<EventId>? =
        streamStore.get(streamKey)

    // eventIds must be the full list, not just the new ones to append
    fun putStreamEventIds(streamKey: StreamKey, eventIds: List<EventId>) {
        streamStore.put(streamKey, eventIds)
    }
}

class StreamStore(override val streamStore: StreamKeyValueStore): IStreamStore

class StreamWithEventsStore(
    override val streamStore: StreamKeyValueStore,
    private val eventStore: EventKeyValueStore
): IStreamStore, StreamWithEventsReadModel {
    override fun streamWithEvents(database: DatabaseName, name: StreamName): StreamWithEvents? {
        val eventIds = streamStore.get(buildStreamKey(database, name))
        return if (eventIds == null)
            null
        else
            StreamWithEvents.create(
                database,
                name,
                eventIds.count().toLong(),
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
