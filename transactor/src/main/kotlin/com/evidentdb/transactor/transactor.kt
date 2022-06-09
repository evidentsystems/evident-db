package com.evidentdb.transactor

import arrow.core.Either
import com.evidentdb.domain.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore

typealias DatabaseKeyValueStore = KeyValueStore<DatabaseId, Database>
typealias DatabaseNameLookupStore = KeyValueStore<DatabaseName, DatabaseId>
typealias StreamKeyValueStore = KeyValueStore<StreamKey, List<EventId>>
typealias EventKeyValueStore = KeyValueStore<EventId, Event>

class KafkaDatabaseReadModel(
    private val databaseStore: DatabaseKeyValueStore,
    private val databaseNameLookupStore: DatabaseNameLookupStore
): DatabaseReadModel {
    override suspend fun get(databaseId: DatabaseId): Database? {
        return databaseStore.get(databaseId)
    }

    override suspend fun get(name: DatabaseName): Database? {
        return databaseStore.get(databaseNameLookupStore.get(name))
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

class KafkaStreamReadModel(
    private val streamStore: StreamKeyValueStore
): StreamReadModel {
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
}

class KafkaStreamsTransactor: Transactor {
    override lateinit var databaseReadModel: DatabaseReadModel
    override lateinit var streamReadModel: StreamReadModel
    lateinit var databaseKeyValueStore: DatabaseKeyValueStore
    lateinit var databaseNameLookupStoreKeyValueStore: DatabaseNameLookupStore
    lateinit var streamKeyValueStore: StreamKeyValueStore

    fun init(
        databaseStore: DatabaseKeyValueStore,
        databaseNameLookupStore: DatabaseNameLookupStore,
        streamStore: StreamKeyValueStore
    ) {
        this.databaseKeyValueStore = databaseStore
        this.databaseNameLookupStoreKeyValueStore = databaseNameLookupStore
        this.streamKeyValueStore = streamStore
        this.databaseReadModel = KafkaDatabaseReadModel(databaseKeyValueStore, databaseNameLookupStoreKeyValueStore)
        this.streamReadModel = KafkaStreamReadModel(streamKeyValueStore)
    }

    // TODO: add storing of result entities
    override fun handleCreateDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> {
        return super.handleCreateDatabase(command)
    }

    // TODO: add storing of result entities
    override fun handleRenameDatabase(command: RenameDatabase): Either<DatabaseRenameError, DatabaseRenamed> {
        return super.handleRenameDatabase(command)
    }

    // TODO: add storing of result entities
    override fun handleDeleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> {
        return super.handleDeleteDatabase(command)
    }

    // TODO: add storing of result entities
    override fun handleTransactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> {
        return super.handleTransactBatch(command)
    }
}
