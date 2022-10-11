package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.right
import arrow.core.rightIfNotNull
import io.cloudevents.CloudEvent
import java.time.Instant

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

interface QueryService {
    companion object {
        // 0 is the protobuf default for int64 fields if not specified
        const val NO_REVISION_SPECIFIED = 0L
    }

    val databaseReadModel: DatabaseReadModel
    val batchReadModel: BatchReadModel
    val streamReadModel: StreamReadModel
    val eventReadModel: EventReadModel

    private fun lookupDatabaseMaybeAtRevision(
        name: DatabaseName,
        revision: DatabaseRevision,
    ): Database? =
        if (revision == NO_REVISION_SPECIFIED)
            databaseReadModel.database(name)
        else
            databaseReadModel.database(name, revision)

    suspend fun getCatalog(): Set<DatabaseSummary> =
        databaseReadModel.catalog()

    suspend fun getDatabase(
        name: String,
        revision: DatabaseRevision,
    ): Either<DatabaseNotFoundError, Database> = either {
        val validName = DatabaseName.of(name)
            .mapLeft { DatabaseNotFoundError(name) }
            .bind()
        lookupDatabaseMaybeAtRevision(validName, revision)
            .rightIfNotNull { DatabaseNotFoundError(name) }
            .bind()
    }

    suspend fun getDatabaseLog(name: String)
            : Either<DatabaseNotFoundError, List<BatchSummary>> = either {
        val validName = DatabaseName.of(name)
            .mapLeft { DatabaseNotFoundError(name) }
            .bind()
        databaseReadModel.log(validName)
            .rightIfNotNull { DatabaseNotFoundError(name) }
            .bind()
    }

    // TODO: support fetching multiple batches at once
    // TODO: ensure batch with id exists w/in database
    suspend fun getBatch(database: String, batchId: BatchId)
            : Either<BatchNotFoundError, Batch> = either {
        val validName = DatabaseName.of(database)
            .mapLeft { BatchNotFoundError(database, batchId) }
            .bind()
        databaseReadModel.exists(validName)
            .rightIfNotNull { BatchNotFoundError(database, batchId) }
            .bind()
        batchReadModel.batch(batchId)
            .rightIfNotNull { BatchNotFoundError(database, batchId) }
            .bind()
    }

    // TODO: ensure Event with id exists w/in database
    suspend fun getEvent(
        database: String,
        eventIds: List<EventId>,
    ) : Either<DatabaseNotFoundError, Map<EventId, CloudEvent>> = either {
        val validName = DatabaseName.of(database)
            .mapLeft { DatabaseNotFoundError(database) }
            .bind()
        databaseReadModel.exists(validName)
            .rightIfNotNull { DatabaseNotFoundError(database) }
            .bind()
        val result = mutableMapOf<EventId, CloudEvent>()
        eventIds.forEach { id ->
            eventReadModel.event(id)?.let {event ->
                result[id] = event
            }
        }
        result
    }

    // TODO: monadic binding for invalid database name
    suspend fun getDatabaseStreams(
        database: String,
        revision: DatabaseRevision,
    ) : Either<DatabaseNotFoundError, Set<StreamSummary>> = either {
        val validName = DatabaseName.of(database)
            .mapLeft { DatabaseNotFoundError(database) }
            .bind()
        val db = lookupDatabaseMaybeAtRevision(validName, revision)
            .rightIfNotNull { DatabaseNotFoundError(database) }
            .bind()
        streamReadModel.databaseStreams(validName)
            .mapNotNull { filterStreamByRevisions(it, db.streamRevisions) }
            .toSet()
    }

    suspend fun getStream(
        database: String,
        databaseRevision: DatabaseRevision,
        streamName: StreamName,
    ) : Either<StreamNotFoundError, Stream> = either {
        val validName = DatabaseName.of(database)
            .mapLeft { StreamNotFoundError(database, streamName) }
            .bind()
        val db = lookupDatabaseMaybeAtRevision(validName, databaseRevision)
            .rightIfNotNull { StreamNotFoundError(database, streamName) }
            .bind()
        val stream = streamReadModel.stream(validName, streamName)
            .rightIfNotNull { StreamNotFoundError(database, streamName) }
            .bind()
        filterStreamByRevisions(stream, db.streamRevisions) as Stream
    }

    suspend fun getSubjectStream(
        database: String,
        databaseRevision: DatabaseRevision,
        stream: StreamName,
        subject: StreamSubject,
    ) : Either<StreamNotFoundError, SubjectStream> =
        TODO("Filter per revision lookup of database streamRevisions")
}