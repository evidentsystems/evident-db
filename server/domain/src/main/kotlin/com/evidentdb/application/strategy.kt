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
) {
    suspend fun createDatabase(nameStr: String): Either<EvidentDbError, NewlyCreatedDatabaseCommandModel> = either {
        val name = DatabaseName(nameStr).bind()
        val initialState = repository.databaseCommandModel(name)
        ensure(initialState is DatabaseCommandModelBeforeCreation) { DatabaseNameAlreadyExists(name) }
        val subscriptionURI = repository.setupDatabase(name).bind()
        val result = execute(initialState, CreateDatabase(EnvelopeId.randomUUID(), name, subscriptionURI)).bind()
        ensure(result is NewlyCreatedDatabaseCommandModel) { DatabaseNameAlreadyExists(name) }
        repository.addDatabase(result).bind()
        result
    }

    suspend fun transactBatch(
        databaseNameStr: String,
        events: List<ProposedEvent>,
        constraints: List<BatchConstraint>
    ): Either<EvidentDbError, ActiveDatabaseCommandModel> = either {
        val databaseName = DatabaseName(databaseNameStr).bind()
        val initialState = repository.databaseCommandModel(databaseName)
        ensure(initialState is ActiveDatabaseCommandModel) { DatabaseNotFound(databaseNameStr) }
        val eventSourceURI = emptyPathEventSourceURI
            .toDatabasePathEventSourceURI(databaseName)
            .bind()
        val batch = ProposedBatch(eventSourceURI, events, constraints)
            .ensureWellFormed()
            .bind()
        val result = execute(
            initialState,
            TransactBatch(EnvelopeId.randomUUID(), databaseName, batch)
        ).bind()
        ensure(result is DirtyDatabaseCommandModel) { IllegalBatchTransactionState(databaseNameStr) }
        repository.saveDatabase(result).bind()
        result
    }

    suspend fun deleteDatabase(nameStr: String): Either<EvidentDbError, DatabaseCommandModelAfterDeletion> = either {
        val name = DatabaseName(nameStr).bind()
        val initialState = repository.databaseCommandModel(name)
        ensure(initialState is ActiveDatabaseCommandModel) { DatabaseNotFound(nameStr) }
        val result = execute(initialState, DeleteDatabase(EnvelopeId.randomUUID(), name)).bind()
        ensure(result is DatabaseCommandModelAfterDeletion) { IllegalDatabaseDeletionState(nameStr) }
        repository.teardownDatabase(name).bind()
        result
    }

    private suspend fun execute(
        initialState: DatabaseCommandModel,
        command: EvidentDbCommand
    ): Either<EvidentDbError, DatabaseCommandModel> = either {
        val events = decider.decide(command, initialState).bind()
        events.fold(initialState) { acc, event ->
            decider.evolve(acc, event)
        }
    }
}
