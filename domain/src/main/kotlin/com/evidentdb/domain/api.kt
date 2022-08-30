package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either

// TODO: pervasive offset tracking from system/tenent-level event-log
//       offset down to database and stream revisions

interface DatabaseReadModel {
    fun exists(name: DatabaseName): Boolean =
        database(name) != null

    fun database(name: DatabaseName): Database?
    fun catalog(): Set<Database>
}

interface BatchReadModel {
    fun batch(database: DatabaseName, id: BatchId): Batch?
    fun batchEventIds(batchKey: BatchKey): List<EventId>?
}

interface StreamReadModel {
    fun streamState(databaseName: DatabaseName, name: StreamName): StreamState
    fun stream(databaseName: DatabaseName, name: StreamName): Stream?

    fun streamEventIds(databaseName: DatabaseName, name: StreamName): List<EventId>? =
        streamEventIds(buildStreamKey(databaseName, name))
    fun streamEventIds(streamKey: StreamKey): List<EventId>?

    fun databaseStreams(databaseName: DatabaseName): Set<Stream>
}

interface StreamWithEventsReadModel: StreamReadModel {
    fun streamWithEvents(database: DatabaseName, name: StreamName): StreamWithEvents?
}

interface EventReadModel {
    fun event(id: EventId): Event?
}

// TODO: Consistency levels!!!
interface Service {
    val databaseReadModel: DatabaseReadModel
    val commandManager: CommandManager

    suspend fun createDatabase(proposedName: String)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val name = DatabaseName.of(proposedName).bind()
            val command = CreateDatabase(
                CommandId.randomUUID(),
                name,
                DatabaseCreationInfo(name)
            )
            commandManager.createDatabase(command).bind()
        }

    suspend fun deleteDatabase(nameStr: String)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val name = DatabaseName.of(nameStr).bind()
            validateDatabaseExists(databaseReadModel, name).bind()
            val command = DeleteDatabase(
                CommandId.randomUUID(),
                name,
                DatabaseDeletionInfo(name)
            )
            commandManager.deleteDatabase(command).bind()
        }

    suspend fun transactBatch(
        databaseNameStr: String,
        events: Iterable<UnvalidatedProposedEvent>
    ): Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseName = DatabaseName.of(databaseNameStr).bind()
            validateDatabaseExists(databaseReadModel, databaseName).bind()
            val validatedEvents = validateUnvalidatedProposedEvents(events).bind()
            val command = TransactBatch(
                CommandId.randomUUID(),
                databaseName,
                ProposedBatch(
                    BatchId.randomUUID(),
                    databaseName,
                    validatedEvents
                )
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
                command.data.name,
            ).bind()
            val database = Database(availableName)
            DatabaseCreated(
                EventId.randomUUID(),
                command.id,
                availableName,
                DatabaseCreationInfo(availableName),
            )
        }

    suspend fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val databaseName = validateDatabaseExists(
                databaseReadModel,
                command.database,
            ).bind()
            DatabaseDeleted(
                EventId.randomUUID(),
                command.id,
                databaseName,
                DatabaseDeletionInfo(databaseName),
            )
        }

    suspend fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseName = validateDatabaseExists(
                databaseReadModel,
                command.database
            ).bind()
            val validBatch = validateProposedBatch(
                databaseName,
                streamReadModel,
                command.data
            ).bind()
            BatchTransacted(
                EventId.randomUUID(),
                command.id,
                databaseName,
                validBatch
            )
        }
}

object EventHandler {
    fun databaseUpdate(event: EventEnvelope)
            : Pair<DatabaseName, Database?>? {
        val database = event.database
        val newDatabase = when (event) {
            is DatabaseCreated -> Database(event.data.name)
            is DatabaseDeleted -> null
            else -> return null
        }

        return Pair(database, newDatabase)
    }

    fun batchToIndex(event: EventEnvelope): Pair<BatchKey, List<EventId>>? {
        return when (event) {
            is BatchTransacted -> Pair(
                buildBatchKey(event.database, event.data.id),
                event.data.events.map { it.id }
            )
            else -> null
        }
    }

    fun streamEventIdsToUpdate(
        streamReadModel: StreamReadModel,
        event: EventEnvelope
    ): LinkedHashMap<StreamKey, List<EventId>>? {
        val database = event.database
        return when (event) {
            is BatchTransacted -> {
                val updates = LinkedHashMap<StreamKey, List<EventId>>()
                for (evt in event.data.events) {
                    val eventIds = updates.getOrPut(
                        buildStreamKey(database, evt.stream!!)
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