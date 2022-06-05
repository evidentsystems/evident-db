package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either

// TODO: pervasive offset tracking from system/tenent-level event-log offset down to database and stream revisions

interface DatabaseStore {
    suspend fun exists(databaseId: DatabaseId): Boolean =
        get(databaseId) != null
    suspend fun exists(name: DatabaseName): Boolean =
        get(name) != null

    suspend fun get(databaseId: DatabaseId): Database?
    suspend fun get(name: DatabaseName): Database?
    suspend fun all(): Iterable<Database>
}

interface StreamStore {
    // Reads from stream definition store
    suspend fun exists(databaseId: DatabaseId, name: StreamName): Boolean =
        get(databaseId, name) != null

    // Reads from both stream definition and stream events stores
    suspend fun streamState(databaseId: DatabaseId, name: StreamName): StreamState
    // Reads from both stream definition and stream events stores
    suspend fun get(databaseId: DatabaseId, name: StreamName): Stream?
    // Reads from stream events store
    suspend fun eventIds(databaseId: DatabaseId, name: StreamName): Iterable<EventId>?
    // Reads from: database streams, stream definition, stream events stores
    suspend fun all(databaseId: DatabaseId): Iterable<Stream>
}

interface EventStore {
    suspend fun get(id: EventId): Event?
}

interface Service {
    val databaseStore: DatabaseStore
    val broker: Broker

    suspend fun createDatabase(proposedName: DatabaseName)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val name = validateDatabaseName(proposedName).bind()
            val command = CreateDatabase(
                CommandId.randomUUID(),
                DatabaseId.randomUUID(),
                DatabaseCreationInfo(name)
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
            val command = RenameDatabase(
                CommandId.randomUUID(),
                databaseId,
                DatabaseRenameInfo(oldName, validNewName)
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
            val validatedEvents = validateUnvalidatedProposedEvents(events).bind()
            val command = TransactBatch(
                CommandId.randomUUID(),
                databaseId,
                ProposedBatch(BatchId.randomUUID(), databaseName, validatedEvents)
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

    suspend fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val availableName = validateDatabaseNameNotTaken(
                databaseStore,
                command.data.name
            ).bind()
            val id = DatabaseId.randomUUID()
            val database = Database(id, availableName)
            DatabaseCreated(EventId.randomUUID(), id, database)
        }

    suspend fun handleRenameDatabase(command: RenameDatabase)
            : Either<DatabaseRenameError, DatabaseRenamed> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseStore,
                command.data.oldName
            ).bind()
            val availableNewName = validateDatabaseNameNotTaken(
                databaseStore,
                command.data.newName
            ).bind()
            DatabaseRenamed(
                EventId.randomUUID(),
                databaseId,
                command.data
            )
        }

    suspend fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseStore,
                command.data.name
            ).bind()
            DatabaseDeleted(EventId.randomUUID(), databaseId, command.data)
        }

    suspend fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseStore,
                command.data.databaseName
            ).bind()
            val transactedBatch = validateProposedBatch(
                databaseId,
                streamStore,
                command.data
            ).bind()
            BatchTransacted(
                EventId.randomUUID(),
                databaseId,
                transactedBatch
            )
        }
}
