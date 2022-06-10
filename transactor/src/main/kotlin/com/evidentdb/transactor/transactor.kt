package com.evidentdb.transactor

import arrow.core.Either
import com.evidentdb.domain.*

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
        this.databaseReadModel = DatabaseStore(databaseKeyValueStore, databaseNameLookupStoreKeyValueStore)
        this.streamReadModel = StreamStore(streamKeyValueStore)
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
