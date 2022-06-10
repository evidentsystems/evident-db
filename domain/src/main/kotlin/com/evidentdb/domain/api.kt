package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either
import kotlinx.coroutines.runBlocking

// TODO: pervasive offset tracking from system/tenent-level event-log offset down to database and stream revisions

interface DatabaseReadModel {
    suspend fun exists(databaseId: DatabaseId): Boolean =
        database(databaseId) != null
    suspend fun exists(name: DatabaseName): Boolean =
        database(name) != null

    suspend fun database(databaseId: DatabaseId): Database?
    suspend fun database(name: DatabaseName): Database?
    suspend fun catalog(): Set<Database>
}

 interface StreamReadModel {
    // Reads from both stream definition and stream events stores
    suspend fun streamState(databaseId: DatabaseId, name: StreamName): StreamState
    // Reads from both stream definition and stream events stores
    suspend fun stream(databaseId: DatabaseId, name: StreamName): Stream?
    // Reads from: database streams, stream definition, stream events stores
    suspend fun databaseStreams(databaseId: DatabaseId): Set<Stream>
}

interface StreamWithEventsReadModel: StreamReadModel {
    suspend fun streamWithEvents(databaseId: DatabaseId, name: StreamName): StreamWithEvents?
}

interface EventReadModel {
    suspend fun batch(id: BatchId): Batch?
    suspend fun event(id: EventId): Event?
}

// TODO: Consistency levels!!!
// TODO: persist batch identity
interface Service {
    val databaseReadModel: DatabaseReadModel
    val commandBroker: CommandBroker

    suspend fun createDatabase(proposedName: DatabaseName)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val name = validateDatabaseName(proposedName).bind()
            val command = CreateDatabase(
                CommandId.randomUUID(),
                DatabaseId.randomUUID(),
                CreateDatabaseInfo(name)
            )
            commandBroker.createDatabase(command).bind()
        }

    suspend fun renameDatabase(
        oldName: DatabaseName,
        newName: DatabaseName
    ): Either<DatabaseRenameError, DatabaseRenamed> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseReadModel,
                oldName
            ).bind()
            val validNewName = validateDatabaseName(newName).bind()
            val command = RenameDatabase(
                CommandId.randomUUID(),
                databaseId,
                DatabaseRenameInfo(oldName, validNewName)
            )
            commandBroker.renameDatabase(command).bind()
        }

    suspend fun deleteDatabase(name: DatabaseName)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseReadModel,
                name
            ).bind()
            val command = DeleteDatabase(
                CommandId.randomUUID(),
                databaseId,
                DatabaseDeletionInfo(name)
            )
            commandBroker.deleteDatabase(command).bind()
        }

    suspend fun transactBatch(
        databaseName: DatabaseName,
        events: Iterable<UnvalidatedProposedEvent>
    ): Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseId = lookupDatabaseIdFromDatabaseName(
                databaseReadModel,
                databaseName
            ).bind()
            val validatedEvents = validateUnvalidatedProposedEvents(events).bind()
            val command = TransactBatch(
                CommandId.randomUUID(),
                databaseId,
                ProposedBatch(BatchId.randomUUID(), databaseName, validatedEvents)
            )
            commandBroker.transactBatch(command).bind()
        }

    suspend fun getCatalog(): Set<Database> = TODO()
    suspend fun getDatabase(name: DatabaseName): Database = TODO()
    suspend fun getDatabaseStreams(name: DatabaseName): Set<Stream> = TODO()
    suspend fun getStream(databaseName: DatabaseName, streamName: StreamName): Stream = TODO()
    suspend fun getStreamEvents(name: DatabaseName, streamName: StreamName): Iterable<Event> = TODO()
    suspend fun getEvent(id: EventId): Event = TODO()
}

interface CommandBroker {
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
    val databaseReadModel: DatabaseReadModel
    val streamReadModel: StreamReadModel

    fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> =
        runBlocking {
            either {
                val availableName = validateDatabaseNameNotTaken(
                    databaseReadModel,
                    command.data.name
                ).bind()
                val id = DatabaseId.randomUUID()
                val database = Database(id, availableName)
                DatabaseCreated(
                    EventId.randomUUID(),
                    command.id,
                    id,
                    DatabaseCreatedInfo(database)
                )
            }
        }

    fun handleRenameDatabase(command: RenameDatabase)
            : Either<DatabaseRenameError, DatabaseRenamed> =
        runBlocking {
            either {
                val databaseId = lookupDatabaseIdFromDatabaseName(
                    databaseReadModel,
                    command.data.oldName
                ).bind()
                validateDatabaseNameNotTaken(
                    databaseReadModel,
                    command.data.newName
                ).bind()
                DatabaseRenamed(
                    EventId.randomUUID(),
                    command.id,
                    databaseId,
                    command.data
                )
            }
        }

    fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        runBlocking {
            either {
                val databaseId = lookupDatabaseIdFromDatabaseName(
                    databaseReadModel,
                    command.data.name
                ).bind()
                DatabaseDeleted(
                    EventId.randomUUID(),
                    command.id,
                    databaseId,
                    command.data
                )
            }
        }

    fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        runBlocking {
            either {
                val databaseId = lookupDatabaseIdFromDatabaseName(
                    databaseReadModel,
                    command.data.databaseName
                ).bind()
                val transactedBatch = validateProposedBatch(
                    databaseId,
                    streamReadModel,
                    command.data
                ).bind()
                BatchTransacted(
                    EventId.randomUUID(),
                    command.id,
                    databaseId,
                    transactedBatch
                )
            }
        }
}
