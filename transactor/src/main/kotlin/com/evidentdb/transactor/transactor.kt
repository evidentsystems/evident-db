package com.evidentdb.transactor

import com.evidentdb.domain.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

typealias DatabaseKeyValueStore = ReadOnlyKeyValueStore<DatabaseId, Database>
typealias DatabaseNameLookup = ReadOnlyKeyValueStore<DatabaseName, DatabaseId>
typealias StreamKeyValueStore = ReadOnlyKeyValueStore<StreamKey, List<EventId>>
typealias EventKeyValueStore = ReadOnlyKeyValueStore<EventId, Event>

class KafkaDatabaseStore(
    private val databaseStore: DatabaseKeyValueStore,
    private val databaseNameLookup: DatabaseNameLookup
): DatabaseStore {
    override suspend fun get(databaseId: DatabaseId): Database? {
        return databaseStore.get(databaseId)
    }

    override suspend fun get(name: DatabaseName): Database? {
        return databaseStore.get(databaseNameLookup.get(name))
    }

    override suspend fun all(): Set<Database> {
        val ret = mutableSetOf<Database>()
        databaseStore.all().use { databaseIterator ->
            for (kv in databaseIterator)
                ret.add(kv.value)
        }
        return ret
    }
}

class KafkaStreamStore(
    private val streamStore: StreamKeyValueStore,
    private val eventStore: EventKeyValueStore
): StreamStore {
    override suspend fun streamState(databaseId: DatabaseId, name: StreamName): StreamState {
        val eventIds = streamStore.get(buildStreamKey(databaseId, name))
        return if (eventIds == null)
            StreamState.NoStream
        else
            StreamState.AtRevision(eventIds.count().toLong())
    }

    override suspend fun get(databaseId: DatabaseId, name: StreamName): Stream? {
        val eventIds = streamStore.get(buildStreamKey(databaseId, name))
        return if (eventIds == null)
            null
        else
            Stream.create(databaseId, name, eventIds.count().toLong())
    }

    override suspend fun getWithEvents(databaseId: DatabaseId, name: StreamName): StreamWithEvents? {
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

    override suspend fun all(databaseId: DatabaseId): Set<Stream> {
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
}

class KafkaStreamsTransactor: Transactor {
    override lateinit var databaseStore: DatabaseStore
    override lateinit var streamStore: StreamStore

    fun init(
        databaseStore: DatabaseKeyValueStore,
        databaseNameLookup: DatabaseNameLookup,
        streamStore: StreamKeyValueStore,
        eventStore: EventKeyValueStore
    ) {
        this.databaseStore = KafkaDatabaseStore(databaseStore, databaseNameLookup)
        this.streamStore = KafkaStreamStore(streamStore, eventStore)
    }
}
