package com.evidentdb.event_model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import java.time.Instant
import java.util.*

typealias EnvelopeId = UUID
typealias EnvelopeType = String

sealed interface EvidentDbCommand {
    val id: EnvelopeId
    val type: EnvelopeType
        get() = "com.evidentdb.command.${this.javaClass.simpleName}"
    val database: DatabaseName

    suspend fun decide(state: DatabaseCommandModel): Either<EvidentDbError, EvidentDbEvent>
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
    override val database: DatabaseName
): EvidentDbEvent {
    override val type: EnvelopeType
        get() = "com.evidentdb.error.${this.javaClass.simpleName}"

    override fun evolve(state: DatabaseCommandModel): DatabaseCommandModel = state
}

// Decide

data class CreateDatabase(
    override val id: EnvelopeId,
    override val database: DatabaseName,
    val topic: TopicName
): EvidentDbCommand {
    override suspend fun decide(state: DatabaseCommandModel): Either<DatabaseCreationError, DatabaseCreated> =
        when (state) {
            is DatabaseCommandModelBeforeCreation -> either {
                val availableName = validateDatabaseNameNotTaken(
                    state,
                    database,
                ).bind()
                DatabaseCreated(
                    EnvelopeId.randomUUID(),
                    id,
                    availableName,
                    state.create(
                        availableName,
                        topic,
                        Instant.now(),
                    ),
                )
            }
            is ActiveDatabaseCommandModel -> DatabaseNameAlreadyExistsError(state.name).left()
            is DatabaseCommandModelAfterDeletion -> DatabaseNameAlreadyExistsError(state.name).left()
        }
}

data class TransactBatch(
    override val id: EnvelopeId,
    override val database: DatabaseName,
    val proposedBatch: ProposedBatch
): EvidentDbCommand {
    override suspend fun decide(state: DatabaseCommandModel) = when (state) {
        is DatabaseCommandModelBeforeCreation -> TODO()
        is ActiveDatabaseCommandModel -> TODO()
        is DatabaseCommandModelAfterDeletion -> TODO()
    }

//    override suspend fun decide(command: EvidentDbCommand): Either<EvidentDbError, EvidentDbEvent> {
//        TODO("Not yet implemented")
//    }
}

data class DeleteDatabase(
    override val id: EnvelopeId,
    override val database: DatabaseName,
): EvidentDbCommand {
    override suspend fun decide(state: DatabaseCommandModel): Either<DatabaseDeletionError, EvidentDbEvent> =
        when (state) {
            is DatabaseCommandModelBeforeCreation -> TODO()
            is ActiveDatabaseCommandModel -> TODO()
            is DatabaseCommandModelAfterDeletion -> TODO()
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
        is DatabaseCommandModelBeforeCreation -> this.createdDatabase
        is ActiveDatabaseCommandModel -> state
        is DatabaseCommandModelAfterDeletion -> state
    }
}

data class BatchTransacted(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    val batch: Batch,
    val revisionBefore: DatabaseRevision,
): EvidentDbEvent {
    override fun evolve(state: DatabaseCommandModel): DatabaseCommandModel = when (state) {
        is DatabaseCommandModelBeforeCreation -> TODO()
        is ActiveDatabaseCommandModel -> TODO()
        is DatabaseCommandModelAfterDeletion -> TODO()
    }

//    override fun evolve(event: EvidentDbEvent): Database = when (event) {
//        is DatabaseCreated -> this
//        is BatchTransacted -> DirtyDatabase(this, event.batch)
//        is DatabaseDeleted -> DatabaseAfterDeletion(name, topic, created, event.deletedAt)
//        is EvidentDbErrorEvent -> this
//    }
}

data class DatabaseDeleted(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    val deletedAt: Instant,
): EvidentDbEvent {
    override fun evolve(state: DatabaseCommandModel): DatabaseCommandModel =
        when (state) {
            is DatabaseCommandModelBeforeCreation -> TODO()
            is ActiveDatabaseCommandModel -> TODO()
            is DatabaseCommandModelAfterDeletion -> TODO()
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

typealias EvidentDbDecider = Decide<EvidentDbCommand, DatabaseCommandModel, EvidentDbEvent, EvidentDbError>

fun decider(
    invariants: DatabaseWriteInvariants
): EvidentDbDecider = Decider(
    { DatabaseCommandModelBeforeCreation(invariants) },
    { state, event -> event.evolve(state) },
    { command, state -> command.decide(state).map { listOf(it) } }
)
