package com.evidentdb.domain

import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.rightIfNotNull
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow
import java.time.Instant

interface CommandService {
    val commandManager: CommandManager

    suspend fun createDatabase(proposedName: String)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val name = DatabaseName.of(proposedName).bind()
            val command = CreateDatabase(
                EnvelopeId.randomUUID(),
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
                EnvelopeId.randomUUID(),
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
            val validatedEvents = validateUnvalidatedProposedEvents(
                databaseName,
                events,
            ).bind()
            val command = TransactBatch(
                EnvelopeId.randomUUID(),
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
            DatabaseCreated(
                EnvelopeId.randomUUID(),
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
            DatabaseDeleted(
                EnvelopeId.randomUUID(),
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
            BatchTransacted(
                EnvelopeId.randomUUID(),
                command.id,
                database.name,
                BatchTransactionResult(
                    validBatch,
                    database,
                )
            )
        }
}

interface DatabaseReadModel {
    fun exists(name: DatabaseName): Boolean =
        database(name) != null
    fun log(name: DatabaseName): Flow<BatchSummary>?
    fun database(name: DatabaseName, revision: DatabaseRevision? = null): Database?
    fun catalog(): Flow<Database>
}

interface BatchSummaryReadModel {
    fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary?
}

interface BatchReadModel: BatchSummaryReadModel {
    fun batch(database: DatabaseName, id: BatchId): Batch?
}

interface StreamReadModel {
    fun streamState(databaseName: DatabaseName, name: StreamName): StreamState
    fun stream(databaseName: DatabaseName, name: StreamName): Flow<EventId>
    fun subjectStream(
        databaseName: DatabaseName,
        name: StreamName,
        subject: StreamSubject,
    ): Flow<EventId>
}

interface EventReadModel {
    fun event(database: DatabaseName, id: EventId): CloudEvent?
    // TODO: event by userspace id?
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
    data class StoreStreams(val updates: List<Pair<StreamKey, EventId>>): StreamOperation
    data class DeleteStreams(val database: DatabaseName): StreamOperation
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
                        batch.streamRevisions,
                        batch.timestamp,
                    )
                }
            )
            is DatabaseDeleted -> BatchOperation.DeleteLog(event.database)
            else -> BatchOperation.DoNothing
        }
    }

    fun streamUpdate(event: EventEnvelope): StreamOperation {
        return when (event) {
            is BatchTransacted -> {
                val database = event.data.databaseBefore.name
                val streamRevisions = event.data.databaseBefore.streamRevisions.toMutableMap()
                var databaseRevision = event.data.databaseBefore.revision
                val updates = mutableListOf<Pair<StreamKey, EventId>>()
                for (evt in event.data.batch.events) {
                    val streamName = evt.stream
                    databaseRevision += 1
                    val streamRevision = (streamRevisions[streamName] ?: 0) + 1
                    streamRevisions[streamName] = streamRevision
                    updates.add(Pair(buildStreamKey(database, streamName, streamRevision), databaseRevision))
                }
                // TODO: remove after verifying
                require(streamRevisions == event.data.databaseAfter.streamRevisions)
                StreamOperation.StoreStreams(updates)
            }
            is DatabaseDeleted -> StreamOperation.DeleteStreams(event.database)
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

    fun lookupDatabaseMaybeAtRevision(
        name: DatabaseName,
        revision: DatabaseRevision,
    ): Database? =
        if (revision == NO_REVISION_SPECIFIED)
            databaseReadModel.database(name)
        else
            databaseReadModel.database(name, revision)

    fun getCatalog(): Flow<Database> =
        databaseReadModel.catalog()

    suspend fun getDatabase(
        name: String,
        revision: DatabaseRevision = NO_REVISION_SPECIFIED,
    ): Either<DatabaseNotFoundError, Database> = either {
        val validName = DatabaseName.of(name)
            .mapLeft { DatabaseNotFoundError(name) }
            .bind()
        lookupDatabaseMaybeAtRevision(validName, revision)
            .rightIfNotNull { DatabaseNotFoundError(name) }
            .bind()
    }

    suspend fun getDatabaseLog(name: String)
            : Either<DatabaseNotFoundError, Flow<BatchSummary>> = either {
        val validName = DatabaseName.of(name)
            .mapLeft { DatabaseNotFoundError(name) }
            .bind()
        databaseReadModel.log(validName)
            .rightIfNotNull { DatabaseNotFoundError(name) }
            .bind()
    }

    suspend fun getBatch(database: String, batchId: BatchId)
            : Either<BatchNotFoundError, Batch> = either {
        val validName = DatabaseName.of(database)
            .mapLeft { BatchNotFoundError(database, batchId) }
            .bind()
        databaseReadModel.exists(validName)
            .rightIfNotNull { BatchNotFoundError(database, batchId) }
            .bind()
        batchReadModel.batch(validName, batchId)
            .rightIfNotNull { BatchNotFoundError(database, batchId) }
            .bind()
    }

    suspend fun getEvent(
        database: String,
        eventId: EventId,
    ) : Either<EventNotFoundError, Pair<EventId, CloudEvent>> = either {
        val error = EventNotFoundError(database, eventId)
        val validName = DatabaseName.of(database)
            .mapLeft { error }
            .bind()
        if (databaseReadModel.exists(validName))
            eventReadModel.event(validName, eventId)?.let { event ->
                Pair(eventId, event)
            }.rightIfNotNull { error }.bind()
        else
            error.left().bind<Pair<EventId, CloudEvent>>()
    }

    suspend fun getStream(
        database: String,
        databaseRevision: DatabaseRevision,
        stream: String,
    ) : Either<StreamNotFoundError, Flow<EventId>> = either {
        val databaseName = DatabaseName.of(database)
            .mapLeft { StreamNotFoundError(database, stream) }
            .bind()
        val streamName = StreamName.of(stream)
            .mapLeft { StreamNotFoundError(database, stream) }
            .bind()
        streamReadModel.stream(databaseName, streamName)
            .rightIfNotNull { StreamNotFoundError(database, stream) }
            .bind()
    }

    suspend fun getSubjectStream(
        database: String,
        databaseRevision: DatabaseRevision,
        stream: StreamName,
        subject: StreamSubject,
    ) : Either<StreamNotFoundError, Flow<EventId>> =
        TODO("Filter per revision lookup of database streamRevisions")
}