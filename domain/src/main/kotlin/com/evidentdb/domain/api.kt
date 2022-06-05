package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either

// TODO: pervasive offset tracking from system/tenent-level event-log offset down to database and stream revisions

interface ReadableDatabaseStore {
    suspend fun exists(databaseId: DatabaseId): Boolean =
        get(databaseId) != null
    suspend fun exists(name: DatabaseName): Boolean =
        get(name) != null

    suspend fun get(databaseId: DatabaseId): Database?
    suspend fun get(name: DatabaseName): Database?
    suspend fun all(): Iterable<Database>
}

// TODO: replace with kstream topic writes?
interface WritableDatabaseStore: ReadableDatabaseStore {
    // Updates both the name -> id and id -> database indexes
    suspend fun create(database: Database): Database
    // Updates both the name -> id and id -> database indexes
    suspend fun delete(databaseId: DatabaseId): DatabaseId
    // Updates just the name -> id index
    suspend fun rename(oldName: DatabaseName, newName: DatabaseName): Database
}

interface ReadableStreamStore {
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

// Replace with kstream aggregation?
interface WritableStreamStore: ReadableStreamStore {
    // Writes to both database streams and stream definition stores
    suspend fun create(databaseId: DatabaseId, name: StreamName): Stream

    // TODO: stream deletion?

    // Writes to stream events store.
    // Assumes event has already been written to event store
    suspend fun append(
        databaseId: DatabaseId,
        name: StreamName,
        eventId: EventId
    ): StreamRevision
}

interface ReadableEventStore {
    suspend fun get(id: EventId): Event?
}

// TODO: replace with emit to event topic keyed on ID?
interface WritableEventStore: ReadableEventStore {
    suspend fun put(event: Event): Event
}

interface Service {
    val databaseStore: ReadableDatabaseStore
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
            val validatedEvents = validateProposedEvents(events).bind()
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
    val databaseStore: WritableDatabaseStore
    val streamStore: WritableStreamStore
    val eventStore: WritableEventStore

    suspend fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val availableName = validateDatabaseNameNotTaken(
                databaseStore,
                command.data.name
            ).bind()
            val id = DatabaseId.randomUUID()
            val database = databaseStore.create(Database(id, availableName))
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
            databaseStore.rename(command.data.oldName, availableNewName)
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
            val deletedId = databaseStore.delete(databaseId)
            DatabaseDeleted(EventId.randomUUID(), deletedId, command.data)
        }

    suspend fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseStore,
                command.data.databaseName
            ).bind()
            val transactedBatch = transactProposedBatch(
                databaseId,
                eventStore,
                streamStore,
                command.data
            ).bind()
            BatchTransacted(EventId.randomUUID(), databaseId, transactedBatch)
        }
}
