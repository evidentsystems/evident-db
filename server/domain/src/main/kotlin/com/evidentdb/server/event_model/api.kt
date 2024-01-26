package com.evidentdb.server.event_model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.evidentdb.server.domain_model.*
import java.time.Instant
import java.util.*

typealias EnvelopeId = UUID
typealias EnvelopeType = String

sealed interface EvidentDbCommand {
    val id: EnvelopeId
    val type: EnvelopeType
        get() = "com.evidentdb.command.${this.javaClass.simpleName}"
    val database: DatabaseName

    suspend fun decide(state: DatabaseCommandModel): Either<EvidentDbCommandError, EvidentDbEvent>
}

sealed interface EvidentDbEvent {
    val id: EnvelopeId
    val type: EnvelopeType
        get() = "com.evidentdb.event.${this.javaClass.simpleName}"
    val commandId: EnvelopeId
    val database: DatabaseName

    fun evolve(state: DatabaseCommandModel): DatabaseCommandModel
}

data class EvidentDbErrorEvent(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    val error: EvidentDbCommandError,
): EvidentDbEvent {
    override val type: EnvelopeType
        get() = "com.evidentdb.error.${this.javaClass.simpleName}"

    override fun evolve(state: DatabaseCommandModel): DatabaseCommandModel = state
}

// Decide

data class CreateDatabase(
    override val id: EnvelopeId,
    override val database: DatabaseName
): EvidentDbCommand {
    override suspend fun decide(state: DatabaseCommandModel): Either<DatabaseCreationError, DatabaseCreated> =
        when (state) {
            is DatabaseCommandModelBeforeCreation -> either {
                ensure(state.databaseNameAvailable(database)) { DatabaseNameAlreadyExists(database) }
                val newlyCreatedDatabase = state.buildNewlyCreatedDatabase(database).bind()
                DatabaseCreated(
                    EnvelopeId.randomUUID(),
                    id,
                    database,
                    newlyCreatedDatabase,
                )
            }
            is ActiveDatabaseCommandModel -> DatabaseNameAlreadyExists(state.name).left()
            is DatabaseCommandModelAfterDeletion -> DatabaseNameAlreadyExists(state.name).left()
        }
}

data class TransactBatch(
    override val id: EnvelopeId,
    override val database: DatabaseName,
    val proposedBatch: WellFormedProposedBatch
): EvidentDbCommand {
    override suspend fun decide(state: DatabaseCommandModel): Either<BatchTransactionError, BatchTransacted> =
        when (state) {
            is ActiveDatabaseCommandModel -> either {
                val acceptedBatch = state.buildAcceptedBatch(proposedBatch).bind()
                BatchTransacted(EnvelopeId.randomUUID(), id, database, acceptedBatch)
            }
            is DatabaseCommandModelBeforeCreation -> DatabaseNotFound(database.value).left()
            is DatabaseCommandModelAfterDeletion -> DatabaseNotFound(database.value).left()
        }
}

data class DeleteDatabase(
    override val id: EnvelopeId,
    override val database: DatabaseName,
): EvidentDbCommand {
    override suspend fun decide(state: DatabaseCommandModel): Either<DatabaseDeletionError, DatabaseDeleted> =
        when (state) {
            is DatabaseCommandModelBeforeCreation -> DatabaseNotFound(database.value).left()
            is DatabaseCommandModelAfterDeletion -> DatabaseNotFound(database.value).left()
            is ActiveDatabaseCommandModel -> either {
                DatabaseDeleted(EnvelopeId.randomUUID(), id, database, Instant.now())
            }
        }
}

// Evolve

data class DatabaseCreated(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    val createdDatabase: NewlyCreatedDatabaseCommandModel
): EvidentDbEvent {
    override fun evolve(state: DatabaseCommandModel): DatabaseCommandModel = when (state) {
        is DatabaseCommandModelBeforeCreation -> createdDatabase
        is ActiveDatabaseCommandModel -> state
        is DatabaseCommandModelAfterDeletion -> state
    }
}

data class BatchTransacted(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    val batch: AcceptedBatch
): EvidentDbEvent {
    override fun evolve(state: DatabaseCommandModel): DatabaseCommandModel = when (state) {
        is DatabaseCommandModelBeforeCreation -> state
        is DatabaseCommandModelAfterDeletion -> state
        is ActiveDatabaseCommandModel -> state.withBatch(batch)
    }
}

data class DatabaseDeleted(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    val deletedAt: Instant,
): EvidentDbEvent {
    override fun evolve(state: DatabaseCommandModel): DatabaseCommandModel =
        when (state) {
            is DatabaseCommandModelBeforeCreation -> state
            is ActiveDatabaseCommandModel -> state.asDeleted()
            is DatabaseCommandModelAfterDeletion -> state
        }
}

// Domain Functions API

interface Evolve<S, in E> {
    val initialState: () -> S
    val evolve: (S, E) -> S
}

interface Decide<in C, S, E, Err>: Evolve<S, E> {
    val decide: suspend (C, S) -> Either<Err, List<E>>
}

interface React<in AR, out A> {
    val react: (AR) -> List<A>
}

data class View<S, in E>(
    override val initialState: () -> S,
    override val evolve: (S, E) -> S,
): Evolve<S, E>

data class Decider<in C, S, E, Err>(
    override val initialState: () -> S,
    override val evolve: (S, E) -> S,
    override val decide: suspend (C, S) -> Either<Err, List<E>>
): Decide<C, S, E, Err>

data class Saga<in Ar, out A>(
    override val react: (Ar) -> List<A>
): React<Ar, A>

typealias EvidentDbDecider = Decide<EvidentDbCommand, DatabaseCommandModel, EvidentDbEvent, EvidentDbCommandError>

fun decider(
    initial: DatabaseCommandModelBeforeCreation
): EvidentDbDecider = Decider(
    { initial },
    { state, event -> event.evolve(state) },
    { command, state -> command.decide(state).map { listOf(it) } }
)
