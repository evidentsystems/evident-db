package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.right
import arrow.core.toOption
import io.cloudevents.CloudEvent
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
    fun log(name: DatabaseName): List<BatchSummary>?
    fun database(name: DatabaseName): Database?
    fun database(name: DatabaseName, revision: DatabaseRevision): Database?
    fun summary(name: DatabaseName): DatabaseSummary?
    fun catalog(): Set<DatabaseSummary>
}

interface BatchSummaryReadModel {
    fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary?
}

interface BatchReadModel: BatchSummaryReadModel {
    fun batch(id: BatchId): Batch?
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
    fun event(id: EventId): CloudEvent?
}

// TODO: as-of revision number option for all database read-model (i.e. stream) queries
// TODO: add subscription/channel-returning function for subscription to a database
interface QueryService {
    val databaseReadModel: DatabaseReadModel
    val batchReadModel: BatchReadModel
    val streamReadModel: StreamReadModel
    val eventReadModel: EventReadModel

    suspend fun getCatalog(): Set<DatabaseSummary> =
        databaseReadModel.catalog()

    // TODO: monadic binding for invalid database name
    suspend fun getDatabase(name: DatabaseName)
            : Either<DatabaseNotFoundError, Database> =
        databaseReadModel.database(name)
                .toOption()
                .toEither { DatabaseNotFoundError(name) }

    // TODO: monadic binding for invalid database name
    suspend fun getDatabaseLog(name: DatabaseName)
            : Either<DatabaseNotFoundError, List<BatchSummary>> =
        databaseReadModel.log(name)
            ?.right()
            ?: DatabaseNotFoundError(name).left()

    suspend fun getBatch(database: DatabaseName, batchId: BatchId)
            : Either<BatchNotFoundError, Batch> =
        batchReadModel.batch(batchId)
            ?.right()
            ?: BatchNotFoundError(database, batchId).left()

    // TODO: monadic binding for invalid database name
    suspend fun getDatabaseStreams(name: DatabaseName)
            : Either<DatabaseNotFoundError, Set<StreamSummary>> =
        if (databaseReadModel.exists(name))
            streamReadModel.databaseStreams(name).right()
        else
            DatabaseNotFoundError(name).left()

    // TODO: monadic binding for invalid database name
    suspend fun getStream(
        databaseName: DatabaseName,
        streamName: StreamName
    ) : Either<StreamNotFoundError, Stream> =
        streamReadModel.stream(databaseName, streamName)
            .toOption()
            .toEither { StreamNotFoundError(databaseName.value, streamName) }

    // TODO: monadic binding for invalid database name
    suspend fun getSubjectStream(
        database: DatabaseName,
        stream: StreamName,
        subject: StreamSubject,
    ) : Either<StreamNotFoundError, SubjectStream> =
        TODO()

    // TODO: monadic binding for invalid database name
    // TODO: key events on EventKey (databaseName@eventId)?
    suspend fun getEvent(
        database: DatabaseName,
        id: EventId,
    ) : Either<EventNotFoundError, CloudEvent> =
        eventReadModel.event(id)
            ?.right()
            ?: EventNotFoundError(database.value, id).left()
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
                        mapOf()
                    )
                ),
            )
        }

    suspend fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val database = lookupDatabase(
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
            val database = lookupDatabase(
                databaseReadModel,
                command.database
            ).bind()
            val validBatch = validateProposedBatch(
                database,
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
                    )
                )
            )
        }
}

sealed interface DatabaseOperation {
    data class StoreDatabase(val databaseSummary: DatabaseSummary): DatabaseOperation
    object DeleteDatabase: DatabaseOperation
    object DoNothing: DatabaseOperation
}

sealed interface BatchOperation {
    data class StoreBatch(val batchSummary: BatchSummary): BatchOperation
    data class DeleteLog(val database: DatabaseName): BatchOperation
    object DoNothing: BatchOperation

}

sealed interface StreamOperation {
    data class StoreStreams(val updates: LinkedHashMap<StreamKey, List<EventId>>): StreamOperation
    data class DeleteStreams(val streams: List<StreamKey>): StreamOperation
    object DoNothing: StreamOperation

}

object EventHandler {
    fun databaseUpdate(event: EventEnvelope): Pair<DatabaseName, DatabaseOperation> =
        Pair(event.database, when(event) {
            is DatabaseCreated -> DatabaseOperation.StoreDatabase(
                event.data.database.let {
                    DatabaseSummary(
                        it.name,
                        it.created
                    )
                }
            )
            is DatabaseDeleted -> DatabaseOperation.DeleteDatabase
            else -> DatabaseOperation.DoNothing
        })

    fun batchUpdate(event: EventEnvelope): BatchOperation {
        return when (event) {
            is BatchTransacted -> BatchOperation.StoreBatch(
                event.data.batch.let { batch ->
                    BatchSummary(
                        batch.id,
                        batch.database,
                        batch.events.map {
                            BatchSummaryEvent(
                                it.id,
                                it.stream
                            )
                        },
                        batch.streamRevisions
                    )
                }
            )
            is DatabaseDeleted -> BatchOperation.DeleteLog(event.database)
            else -> BatchOperation.DoNothing
        }
    }

    fun streamUpdate(
        streamSummaryReadModel: StreamSummaryReadModel,
        event: EventEnvelope
    ): StreamOperation {
        val database = event.database
        return when (event) {
            is BatchTransacted -> {
                val updates = LinkedHashMap<StreamKey, List<EventId>>()
                for (evt in event.data.batch.events) {
                    val eventIds = updates.getOrPut(
                        buildStreamKey(database, evt.stream)
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
                StreamOperation.StoreStreams(result)
            }
            is DatabaseDeleted -> StreamOperation.DeleteStreams(
                streamSummaryReadModel.databaseStreams(database)
                    .map { buildStreamKey(database, it.name) }
            )
            else -> StreamOperation.DoNothing
        }
    }

    fun eventsToIndex(event: EventEnvelope): List<Pair<EventId, Event>>? =
        when (event) {
            is BatchTransacted -> event.data.batch.events.map { Pair(it.id, it) }
            else -> null
        }
}