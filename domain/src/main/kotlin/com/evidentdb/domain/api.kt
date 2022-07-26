package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either

// TODO: pervasive offset tracking from system/tenent-level event-log offset down to database and stream revisions

interface DatabaseReadModel {
    fun exists(databaseId: DatabaseId): Boolean =
        database(databaseId) != null
    fun exists(name: DatabaseName): Boolean =
        database(name) != null

    fun database(databaseId: DatabaseId): Database?
    fun database(name: DatabaseName): Database?
    fun catalog(): Set<Database>
}

interface BatchReadModel {
    fun batch(databaseId: DatabaseId, id: BatchId): Batch?
    fun batchEventIds(batchKey: BatchKey): List<EventId>?
}

interface StreamReadModel {
    fun streamState(databaseId: DatabaseId, name: StreamName): StreamState
    fun stream(databaseId: DatabaseId, name: StreamName): Stream?
    fun streamEventIds(streamKey: StreamKey): List<EventId>?
    fun databaseStreams(databaseId: DatabaseId): Set<Stream>
}

interface StreamWithEventsReadModel: StreamReadModel {
    fun streamWithEvents(databaseId: DatabaseId, name: StreamName): StreamWithEvents?
}

interface EventReadModel {
    fun event(id: EventId): Event?
}

// TODO: Consistency levels!!!
interface Service {
    val databaseReadModel: DatabaseReadModel
    val commandManager: CommandManager

    suspend fun createDatabase(proposedName: DatabaseName)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val name = validateDatabaseName(proposedName).bind()
            val command = CreateDatabase(
                CommandId.randomUUID(),
                DatabaseId.randomUUID(),
                DatabaseCreationInfo(name)
            )
            commandManager.createDatabase(command).bind()
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
            commandManager.renameDatabase(command).bind()
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
            commandManager.deleteDatabase(command).bind()
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
                ProposedBatch(BatchId.randomUUID(), databaseId, validatedEvents)
            )
            commandManager.transactBatch(command).bind()
        }

    suspend fun getCatalog(): Set<Database> = TODO()
    suspend fun getDatabase(name: DatabaseName): Database = TODO()
    suspend fun getDatabaseStreams(name: DatabaseName): Set<Stream> = TODO()
    suspend fun getStream(databaseName: DatabaseName, streamName: StreamName): Stream = TODO()
    suspend fun getStreamEvents(name: DatabaseName, streamName: StreamName): List<Event> = TODO()
    suspend fun getEvent(id: EventId): Event = TODO()
}

interface CommandManager {
    suspend fun createDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated>
    suspend fun renameDatabase(command: RenameDatabase)
            : Either<DatabaseRenameError, DatabaseRenamed>
    suspend fun deleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted>
    suspend fun transactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted>
}

interface CommandHandler {
    val databaseReadModel: DatabaseReadModel
    val streamReadModel: StreamReadModel

    suspend fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val availableName = validateDatabaseNameNotTaken(
                databaseReadModel,
                command.data.name
            ).bind()
            val id = command.databaseId
            val database = Database(id, availableName)
            DatabaseCreated(
                EventId.randomUUID(),
                command.id,
                id,
                DatabaseCreatedInfo(database)
            )
        }

    suspend fun handleRenameDatabase(command: RenameDatabase)
            : Either<DatabaseRenameError, DatabaseRenamed> =
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

    suspend fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
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

    suspend fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseId = command.data.databaseId
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

object EventHandler {
    fun databaseUpdate(event: EventEnvelope)
            : Pair<DatabaseId, Database?>? {
        val databaseId = event.databaseId
        val newDatabase = when (event) {
            is DatabaseCreated -> event.data.database
            is DatabaseDeleted -> null
            is DatabaseRenamed -> Database(
                databaseId,
                event.data.newName
            )
            else -> return null
        }

        return Pair(databaseId, newDatabase)
    }

    fun databaseNameLookupUpdate(event: EventEnvelope)
            : Pair<DatabaseName, DatabaseId?>? {
        return when (event) {
            is DatabaseCreated -> Pair(
                event.data.database.name,
                event.databaseId
            )
            is DatabaseDeleted -> Pair(
                event.data.name,
                null
            )
            is DatabaseRenamed -> Pair(
                event.data.newName,
                event.databaseId
            )
            else -> return null
        }
    }

    fun batchToIndex(event: EventEnvelope): Pair<BatchKey, List<EventId>>? {
        return when (event) {
            is BatchTransacted -> Pair(
                buildBatchKey(event.databaseId, event.data.id),
                event.data.events.map { it.id }
            )
            else -> null
        }
    }

    fun streamEventIdsToUpdate(
        streamReadModel: StreamReadModel,
        event: EventEnvelope
    ): LinkedHashMap<StreamKey, List<EventId>>? {
        val databaseId = event.databaseId
        return when (event) {
            is BatchTransacted -> {
                val updates = LinkedHashMap<StreamKey, List<EventId>>()
                for (evt in event.data.events) {
                    val eventIds = updates.getOrPut(
                        buildStreamKey(databaseId, evt.stream!!)
                    ) { mutableListOf() } as MutableList<EventId>
                    eventIds.add(evt.id)
                }
                val result = LinkedHashMap<StreamKey, List<EventId>>(
                    event.data.events.size
                )
                for ((streamKey, eventIds) in updates) {
                    val idempotentEventIds = LinkedHashSet(
                        streamReadModel
                            .streamEventIds(streamKey)
                            .orEmpty()
                    )
                    for (eventId in eventIds)
                        idempotentEventIds.add(eventId)
                    result[streamKey] = idempotentEventIds.toList()
                }
                result
            }
            else -> null
        }
    }

    fun eventsToIndex(event: EventEnvelope): List<Pair<EventId, Event>>? =
        when (event) {
            is BatchTransacted -> event.data.events.map { Pair(it.id, it) }
            else -> null
        }
}