package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either

interface DatabaseStore {
    val revision: DatabaseRevision

    suspend fun exists(databaseId: DatabaseId): Boolean
    suspend fun exists(name: DatabaseName): Boolean
    suspend fun get(databaseId: DatabaseId): Database?
    suspend fun get(name: DatabaseName): Database?
    suspend fun all(): Iterable<Database>
}

interface StreamStore {
    val database: Database

    suspend fun exists(stream: String): Boolean
    suspend fun streamRevision(stream: String): StreamRevision
    suspend fun get(name: StreamName): Stream?
    suspend fun all(): Iterable<Stream>
}

interface EventStore {
    suspend fun get(id: EventId): Event?
    suspend fun all(): Iterable<Event>
}

// TODO: consider accumulating errors, rather than failing fast
interface Service {
    val databaseStore: DatabaseStore
    val broker: Broker

    suspend fun createDatabase(proposedName: DatabaseName)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val name = validateDatabaseName(proposedName).bind()
            val availableName = validateDatabaseNameNotTaken(databaseStore, name).bind()
            val command = CreateDatabase(
                CommandId.randomUUID(),
                DatabaseId.randomUUID(),
                DatabaseCreationInfo(availableName)
            )
            broker.createDatabase(command).bind()
        }

    suspend fun renameDatabase(
        oldName: DatabaseName,
        newName: DatabaseName
    ): Either<DatabaseRenameError, DatabaseRenamed> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseStore,
                oldName
            ).bind()
            val validNewName = validateDatabaseName(newName).bind()
            val availableNewName = validateDatabaseNameNotTaken(databaseStore, validNewName)
                .bind()
            val command = RenameDatabase(
                CommandId.randomUUID(),
                databaseId,
                DatabaseRenameInfo(oldName, availableNewName)
            )
            broker.renameDatabase(command).bind()
        }

    suspend fun deleteDatabase(name: DatabaseName)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseStore,
                name
            ).bind()
            val command = DeleteDatabase(
                CommandId.randomUUID(),
                databaseId,
                DatabaseDeletionInfo(name)
            )
            broker.deleteDatabase(command).bind()
        }

    suspend fun transactBatch(
        databaseName: DatabaseName,
        events: Iterable<UnvalidatedProposedEvent>
    ): Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseStore,
                databaseName
            ).bind()
            val validatedEvents = validateProposedEvents(events).bind()
            val command = TransactBatch(
                CommandId.randomUUID(),
                databaseId,
                ProposedBatch(BatchId.randomUUID(), validatedEvents)
            )
            broker.transactBatch(command).bind()
        }
}

interface Broker {
    suspend fun createDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated>
    suspend fun renameDatabase(command: RenameDatabase)
            : Either<DatabaseRenameError, DatabaseRenamed>
    suspend fun deleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted>
    suspend fun transactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted>
}

interface Transactor {
    val databaseStore: DatabaseStore
    val streamStore: StreamStore

    fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> =
        TODO()

    fun handleRenameDatabase(command: RenameDatabase)
            : Either<DatabaseRenameError, DatabaseRenamed> =
        TODO()

    fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        TODO()

    fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        TODO()
}
