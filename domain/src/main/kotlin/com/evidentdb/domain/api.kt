package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.right
import java.time.Instant

// TODO: Consistency levels!!!
interface CommandService {
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
}

interface DatabaseReadModel {
    fun exists(name: DatabaseName): Boolean =
        database(name) != null
    fun database(name: DatabaseName): Database?
    fun catalog(): Set<Database>
}

interface BatchSummaryReadModel {
    fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary?
}

interface BatchReadModel: BatchSummaryReadModel {
    fun batch(database: DatabaseName, id: BatchId): Batch?
}

interface StreamSummaryReadModel {
    fun streamState(databaseName: DatabaseName, name: StreamName): StreamState
    fun databaseStreams(databaseName: DatabaseName): Set<StreamSummary>
    fun streamSummary(streamKey: StreamKey): StreamSummary?
    fun streamSummary(databaseName: DatabaseName, name: StreamName): StreamSummary? =
        streamSummary(buildStreamKey(databaseName, name))
}

interface StreamReadModel: StreamSummaryReadModel {
    fun stream(databaseName: DatabaseName, name: StreamName): Stream?
}

interface EventReadModel {
    fun event(id: EventId): Event?
}

interface QueryService {
    val databaseReadModel: DatabaseReadModel
    val batchReadModel: BatchReadModel
    val streamReadModel: StreamReadModel
    val eventReadModel: EventReadModel

    suspend fun getCatalog(): Set<Database> =
        databaseReadModel.catalog()

    suspend fun getDatabase(name: DatabaseName): Either<DatabaseNotFoundError, Database> =
        databaseReadModel.database(name)
            ?.right()
            ?: DatabaseNotFoundError(name).left()

    suspend fun getDatabaseBatches(name: DatabaseName)
            : Either<DatabaseNotFoundError, List<BatchSummary>> = TODO()

    suspend fun getBatch(database: DatabaseName, batchId: BatchId)
            : Either<BatchNotFoundError, Batch> =
        batchReadModel.batch(database, batchId)?.right() ?: BatchNotFoundError(batchId).left()

    suspend fun getDatabaseStreams(name: DatabaseName): Set<Stream> = TODO()
    suspend fun getStream(databaseName: DatabaseName, streamName: StreamName): Stream? = TODO()
    suspend fun getSubjectStream(name: DatabaseName, streamName: StreamName, subject: StreamSubject): SubjectStreamSummary = TODO()
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
    val streamSummaryReadModel: StreamSummaryReadModel
    val batchSummaryReadModel: BatchSummaryReadModel

    suspend fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val availableName = validateDatabaseNameNotTaken(
                databaseReadModel,
                command.data.name,
            ).bind()
            val eventId = EventId.randomUUID()
            DatabaseCreated(
                eventId,
                command.id,
                availableName,
                DatabaseCreationResult(
                    Database(
                        availableName,
                        Instant.now(),
                        eventId,
                    )
                ),
            )
        }

    suspend fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val database = validateDatabaseExists(
                databaseReadModel,
                command.database,
            ).bind()
            val eventId = EventId.randomUUID()
            DatabaseDeleted(
                eventId,
                command.id,
                database.name,
                DatabaseDeletionResult(database),
            )
        }

    suspend fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        either {
            val database = validateDatabaseExists(
                databaseReadModel,
                command.database
            ).bind()
            val validBatch = validateProposedBatch(
                database.name,
                streamSummaryReadModel,
                batchSummaryReadModel,
                command.data
            ).bind()
            val eventId = EventId.randomUUID()
            BatchTransacted(
                eventId,
                command.id,
                database.name,
                BatchTransactionResult(
                    validBatch,
                    databaseAfterBatchTransacted(
                        database,
                        validBatch,
                        eventId,
                    )
                )
            )
        }
}

sealed interface DatabaseOperation {
    data class StoreDatabase(val database: Database): DatabaseOperation
    object DeleteDatabase: DatabaseOperation
    object DoNothing: DatabaseOperation
}

object EventHandler {
    fun databaseUpdate(event: EventEnvelope): Pair<DatabaseName, DatabaseOperation> =
        Pair(event.database, when(event) {
            is DatabaseCreated -> DatabaseOperation.StoreDatabase(event.data.database)
            is DatabaseDeleted -> DatabaseOperation.DeleteDatabase
            is BatchTransacted -> DatabaseOperation.StoreDatabase(event.data.database)
            is ErrorEnvelope -> DatabaseOperation.DoNothing
        })

    fun batchToIndex(event: EventEnvelope): Pair<BatchId, List<EventId>>? {
        return when (event) {
            is BatchTransacted -> Pair(
                event.data.batch.id,
                event.data.batch.events.map { it.id }
            )
            else -> null
        }
    }

    fun streamEventIdsToUpdate(
        streamSummaryReadModel: StreamSummaryReadModel,
        event: EventEnvelope
    ): LinkedHashMap<StreamKey, List<EventId>>? {
        val database = event.database
        return when (event) {
            is BatchTransacted -> {
                val updates = LinkedHashMap<StreamKey, List<EventId>>()
                for (evt in event.data.batch.events) {
                    val eventIds = updates.getOrPut(
                        buildStreamKey(database, evt.stream!!)
                    ) { mutableListOf() } as MutableList<EventId>
                    eventIds.add(evt.id)
                }
                val result = LinkedHashMap<StreamKey, List<EventId>>(
                    event.data.batch.events.size
                )
                for ((streamKey, eventIds) in updates) {
                    val idempotentEventIds = LinkedHashSet(
                        streamSummaryReadModel
                            .streamSummary(streamKey)
                            ?.eventIds
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
            is BatchTransacted -> event.data.batch.events.map { Pair(it.id, it) }
            else -> null
        }
}