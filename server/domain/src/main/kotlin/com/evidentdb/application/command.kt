package com.evidentdb.application

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.evidentdb.domain_model.*
import com.evidentdb.event_model.*

class CommandService(
    private val decider: EvidentDbDecider,
    private val repository: WritableDatabaseRepository,
    private val emptyPathEventSourceURI: EmptyPathEventSourceURI,
): Lifecycle {
    override fun setup(params: Map<String, String>) {
        repository.setup(params)
    }

    override fun teardown() {
        repository.teardown()
    }

    suspend fun createDatabase(nameStr: String): Either<EvidentDbCommandError, NewlyCreatedDatabaseCommandModel> = either {
        val name = DatabaseName(nameStr).bind()
        val initialState = repository.databaseCommandModel(name)
        ensure(initialState is DatabaseCommandModelBeforeCreation) { DatabaseNameAlreadyExists(name) }
        val result = execute(initialState, CreateDatabase(EnvelopeId.randomUUID(), name)).bind()
        ensure(result is NewlyCreatedDatabaseCommandModel) { DatabaseNameAlreadyExists(name) }
        repository.setupDatabase(result).bind()
        result
    }

    suspend fun transactBatch(
        databaseNameStr: String,
        events: List<ProposedEvent>,
        constraints: List<BatchConstraint>
    ): Either<EvidentDbCommandError, AcceptedBatch> = either {
        val databaseName = DatabaseName(databaseNameStr).bind()
        val initialState = repository.databaseCommandModel(databaseName)
        ensure(initialState is ActiveDatabaseCommandModel) { DatabaseNotFound(databaseNameStr) }
        val eventSourceURI = emptyPathEventSourceURI
            .toDatabasePathEventSourceURI(databaseName)
            .mapLeft {
                InternalServerError(
                    "Invalid database URI from base $emptyPathEventSourceURI " +
                        "and database $databaseName"
                )
            }.bind()
        val batch = ProposedBatch(eventSourceURI, events, constraints)
            .ensureWellFormed()
            .bind()
        val result = execute(
            initialState,
            TransactBatch(EnvelopeId.randomUUID(), databaseName, batch)
        ).bind()
        ensure(result is DirtyDatabaseCommandModel) {
            InternalServerError("Illegal state when applying batch to $databaseNameStr")
        }
        repository.saveDatabase(result).bind()
        result.dirtyBatches.first()
    }

    suspend fun deleteDatabase(nameStr: String): Either<EvidentDbCommandError, DatabaseCommandModelAfterDeletion> = either {
        val name = DatabaseName(nameStr).bind()
        val initialState = repository.databaseCommandModel(name)
        ensure(initialState is ActiveDatabaseCommandModel) { DatabaseNotFound(nameStr) }
        val result = execute(initialState, DeleteDatabase(EnvelopeId.randomUUID(), name)).bind()
        ensure(result is DatabaseCommandModelAfterDeletion) {
            InternalServerError("Illegal state when deleting $nameStr")
        }
        repository.teardownDatabase(result).bind()
        result
    }

    private suspend fun execute(
        initialState: DatabaseCommandModel,
        command: EvidentDbCommand
    ): Either<EvidentDbCommandError, DatabaseCommandModel> = either {
        val events = decider.decide(command, initialState).bind()
        events.fold(initialState) { acc, event ->
            decider.evolve(acc, event)
        }
    }
}

interface WritableDatabaseRepository: DatabaseRepository {
    suspend fun databaseCommandModel(name: DatabaseName): DatabaseCommandModel

    // Database Creation
    suspend fun setupDatabase(database: NewlyCreatedDatabaseCommandModel): Either<DatabaseCreationError, Unit>

    // Batch Transaction
    suspend fun saveDatabase(database: DirtyDatabaseCommandModel): Either<BatchTransactionError, Unit>

    // Database Deletion
    suspend fun teardownDatabase(database: DatabaseCommandModelAfterDeletion): Either<DatabaseDeletionError, Unit>
}