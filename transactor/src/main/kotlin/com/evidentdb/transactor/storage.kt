package com.evidentdb.transactor

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import com.evidentdb.domain.*

typealias DatabaseKeyValueStore = KeyValueStore<DatabaseId, Database>
typealias DatabaseNameLookupStore = KeyValueStore<DatabaseName, DatabaseId>
typealias StreamKeyValueStore = KeyValueStore<StreamKey, List<EventId>>
typealias EventKeyValueStore = KeyValueStore<EventId, Event>

class DatabaseStore(
    private val databaseStore: DatabaseKeyValueStore,
    private val databaseNameLookupStore: DatabaseNameLookupStore
): DatabaseReadModel {
    override suspend fun database(databaseId: DatabaseId): Database? {
        return databaseStore.get(databaseId)
    }

    override suspend fun database(name: DatabaseName): Database? {
        return databaseStore.get(databaseNameLookupStore.get(name))
    }

    override suspend fun catalog(): Set<Database> {
        val ret = mutableSetOf<Database>()
        databaseStore.all().use { databaseIterator ->
            for (kv in databaseIterator)
                ret.add(kv.value)
        }
        return ret
    }

    suspend fun putDatabase(databaseId: DatabaseId, database: Database) {
        databaseStore.put(databaseId, database)
        databaseNameLookupStore.put(database.name, database.id)
    }
}

interface IStreamStore: StreamReadModel {
    val streamStore: StreamKeyValueStore

    override suspend fun streamState(databaseId: DatabaseId, name: StreamName): StreamState {
        val eventIds = streamStore.get(buildStreamKey(databaseId, name))
        return if (eventIds == null)
            StreamState.NoStream
        else
            StreamState.AtRevision(eventIds.count().toLong())
    }

    override suspend fun stream(databaseId: DatabaseId, name: StreamName): Stream? {
        val eventIds = streamStore.get(buildStreamKey(databaseId, name))
        return if (eventIds == null)
            null
        else
            Stream.create(databaseId, name, eventIds.count().toLong())
    }

    override suspend fun databaseStreams(databaseId: DatabaseId): Set<Stream> {
        val ret = mutableSetOf<Stream>()
        streamStore.prefixScan(databaseId, Serdes.UUID().serializer())
            .use { streamIterator ->
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

    suspend fun putStream(databaseId: DatabaseId, streamName: StreamName, eventIds: List<EventId>) {
        streamStore.put(buildStreamKey(databaseId, streamName), eventIds)
    }
}

class StreamStore(override val streamStore: StreamKeyValueStore): IStreamStore

class StreamWithEventsStore(
    override val streamStore: StreamKeyValueStore,
    private val eventStore: EventKeyValueStore
): IStreamStore, StreamWithEventsReadModel {
    override suspend fun streamWithEvents(databaseId: DatabaseId, name: StreamName): StreamWithEvents? {
        val eventIds = streamStore.get(buildStreamKey(databaseId, name))
        return if (eventIds == null)
            null
        else
            StreamWithEvents.create(
                databaseId,
                name,
                eventIds.count().toLong(),
                eventIds.map { eventStore.get(it)!! }
            )
    }
}

class EventStore(
    private val eventStore: EventKeyValueStore
): EventReadModel {
    override suspend fun batch(id: BatchId): Batch? {
        TODO("Not yet implemented")
    }

    override suspend fun event(id: EventId): Event? {
        TODO("Not yet implemented")
    }

    suspend fun putEvent(eventId: EventId, event: Event) {
        eventStore.put(eventId, event)
    }
}
