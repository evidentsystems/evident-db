package com.evidentdb.domain_model.command

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.evidentdb.domain_model.*
import java.time.Instant

sealed interface DatabaseCommandModel {
    val name: DatabaseName

    suspend fun ensureCreate(name: DatabaseName): Boolean
    suspend fun databaseNameAvailable(name: DatabaseName): Boolean
    suspend fun batchIdAvailable(name: DatabaseName, batchId: BatchId): Boolean
    suspend fun streamState(
        name: DatabaseName,
        streamName: StreamName,
        subject: EventSubject? = null,
    ): StreamState
    suspend fun eventIdAvailable(name: DatabaseName, eventId: EventId): Boolean
    suspend fun afterDelete(name: DatabaseName): Boolean
}

// Adapter API

interface DatabaseCommandModelBeforeCreation: DatabaseCommandModel {
    suspend fun create(
        topic: TopicName,
        created: Instant
    ): Either<DatabaseCreationError, NewlyCreatedDatabaseCommandModel> = either {
        ensure(databaseNameAvailable(name)) { DatabaseNameAlreadyExistsError(name) }
        ensure(ensureCreate(name)) { InternalServerError("Failed to create database") }
        NewlyCreatedDatabaseCommandModel(topic, created, this@DatabaseCommandModelBeforeCreation)
    }
}

interface CleanDatabaseCommandModel: ActiveDatabaseCommandModel

// Domain API

sealed interface ActiveDatabaseCommandModel: DatabaseCommandModel {
    val topic: TopicName
    val created: Instant
    val clock: DatabaseClock
    val revision: DatabaseRevision

    fun acceptBatch(proposedBatch: ProposedBatch): Either<BatchTransactionError, DirtyDatabaseCommandModel> =
        either {
            val batch = proposedBatch.validate(this@ActiveDatabaseCommandModel).bind()
            DirtyDatabaseCommandModel(this@ActiveDatabaseCommandModel, batch)
        }

    fun deleted(): Either<DatabaseDeletionError, DatabaseCommandModelAfterDeletion> =
        either {
            // TODO: validations?
            DatabaseCommandModelAfterDeletion(this@ActiveDatabaseCommandModel)
        }
}

data class NewlyCreatedDatabaseCommandModel(
    override val topic: TopicName,
    override val created: Instant,
    private val basis: DatabaseCommandModelBeforeCreation,
): ActiveDatabaseCommandModel {
    override val name: DatabaseName
        get() = basis.name
    override val clock
        get() = DatabaseClock.EMPTY
    override val revision
        get() = 0L

    override suspend fun ensureCreate(name: DatabaseName): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun databaseNameAvailable(name: DatabaseName): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun batchIdAvailable(name: DatabaseName, batchId: BatchId): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun streamState(name: DatabaseName, streamName: StreamName, subject: EventSubject?): StreamState {
        TODO("Not yet implemented")
    }

    override suspend fun eventIdAvailable(name: DatabaseName, eventId: EventId): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun afterDelete(name: DatabaseName): Boolean {
        TODO("Not yet implemented")
    }
}

data class DirtyDatabaseCommandModel internal constructor(
    private val basis: ActiveDatabaseCommandModel,
    private val batch: Batch,
) : ActiveDatabaseCommandModel by basis {
    override val revision: DatabaseRevision = batch.revisionAfter
    override val clock = basis.clock.advance(batch.events)

     val batches: List<Batch>
        get() = if (basis is DirtyDatabaseCommandModel) {
            val result = basis.batches.toMutableList()
            result.add(batch)
            result
        } else {
            listOf(batch)
        }
}

data class DatabaseCommandModelAfterDeletion(
    private val basis: ActiveDatabaseCommandModel
): DatabaseCommandModel {
    override val name: DatabaseName
        get() = basis.name

    override suspend fun ensureCreate(name: DatabaseName): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun databaseNameAvailable(name: DatabaseName): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun batchIdAvailable(name: DatabaseName, batchId: BatchId): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun streamState(name: DatabaseName, streamName: StreamName, subject: EventSubject?): StreamState {
        TODO("Not yet implemented")
    }

    override suspend fun eventIdAvailable(name: DatabaseName, eventId: EventId): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun afterDelete(name: DatabaseName): Boolean {
        TODO("Not yet implemented")
    }
}
