package com.evidentdb.adapter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.resilience.Schedule
import com.evidentdb.application.CommandService
import com.evidentdb.application.DatabaseRepository
import com.evidentdb.domain_model.*
import kotlinx.coroutines.flow.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private fun isNotWriteCollision(transactionResult: Either<EvidentDbCommandError, AcceptedBatch>) =
    when (transactionResult) {
        is Either.Left -> transactionResult.value !is ConcurrentWriteCollision
        is Either.Right -> true
    }

private val batchTransactRetrySchedule = Schedule
    .exponential<Either<EvidentDbCommandError, AcceptedBatch>>(10.milliseconds)
    .doUntil { result, duration -> duration > 60.seconds || isNotWriteCollision(result) }

interface EvidentDbAdapter {
    val commandService: CommandService
    val repository: DatabaseRepository

    // Command API
    suspend fun createDatabase(nameStr: String): Either<EvidentDbCommandError, Database> =
        commandService.createDatabase(nameStr)

    /**
     * This function retries per `batchTransactRetrySchedule` in the event of write collision
     */
    suspend fun transactBatch(
        databaseNameStr: String,
        events: List<ProposedEvent>,
        constraints: List<BatchConstraint>
    ): Either<EvidentDbCommandError, AcceptedBatch> {
        var result: Either<EvidentDbCommandError, AcceptedBatch> =
            ConcurrentWriteCollision(0uL, 0uL).left()
        batchTransactRetrySchedule.repeat {
            result = commandService.transactBatch(databaseNameStr, events, constraints)
            result
        }
        return result
    }

    suspend fun deleteDatabase(nameStr: String): Either<EvidentDbCommandError, Database> =
        commandService.deleteDatabase(nameStr)

    // Query API
    suspend fun catalog(): Flow<DatabaseName> = repository.databaseCatalog()

    fun connect(databaseNameStr: String): Flow<Either<QueryError, Database>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr).mapLeft { DatabaseNotFound(databaseNameStr) }) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> when (val database = repository.latestDatabase(databaseName.value)) {
                is Either.Left -> emit(database)
                is Either.Right -> emitAll(repository.subscribe(database.value.name))
            }
        }
    }

    suspend fun latestDatabase(databaseNameStr: String): Either<QueryError, Database> = either {
        val databaseName = DatabaseName(databaseNameStr)
            .mapLeft { DatabaseNotFound(databaseNameStr) }
            .bind()
        repository.latestDatabase(databaseName).bind()
    }

    suspend fun databaseAtRevision(
        databaseNameStr: String,
        revision: DatabaseRevision,
    ): Either<QueryError, Database> = either {
        val databaseName = DatabaseName(databaseNameStr)
            .mapLeft { DatabaseNotFound(databaseNameStr) }
            .bind()
        repository.databaseAtRevision(databaseName, revision).bind()
    }

    fun databaseLog(
        databaseNameStr: String,
        revision: DatabaseRevision,
    ): Flow<Either<QueryError, Batch>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr).mapLeft { DatabaseNotFound(databaseNameStr) }) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> {
                when (val database = repository.databaseAtRevision(databaseName.value, revision)) {
                    is Either.Left -> emit(database)
                    is Either.Right -> emitAll(database.value.log().map { it.right() })
                }
            }
        }
    }

    fun stream(
        databaseNameStr: String,
        revision: DatabaseRevision,
        streamNameStr: String,
    ): Flow<Either<QueryError, EventRevision>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr).mapLeft { DatabaseNotFound(databaseNameStr) }) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> {
                when (val streamName = StreamName(streamNameStr)) {
                    is Either.Left -> emit(streamName)
                    is Either.Right -> {
                        when (val database = repository.databaseAtRevision(databaseName.value, revision)) {
                            is Either.Left -> emit(database)
                            is Either.Right -> emitAll(database.value.stream(streamName.value).map { it.right() })
                        }
                    }
                }
            }
        }
    }


    fun subjectStream(
        databaseNameStr: String,
        revision: DatabaseRevision,
        streamNameStr: String,
        subjectStr: String,
    ): Flow<Either<QueryError, EventRevision>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr).mapLeft { DatabaseNotFound(databaseNameStr) }) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> {
                when (val streamName = StreamName(streamNameStr)) {
                    is Either.Left -> emit(streamName)
                    is Either.Right -> {
                        when (val subject = EventSubject(subjectStr)) {
                            is Either.Left -> emit(subject)
                            is Either.Right -> {
                                when (val database = repository.databaseAtRevision(databaseName.value, revision)) {
                                    is Either.Left -> emit(database)
                                    is Either.Right -> emitAll(
                                        database.value.subjectStream(streamName.value, subject.value)
                                            .map { it.right() }
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fun subject(
        databaseNameStr: String,
        revision: DatabaseRevision,
        subjectStr: String,
    ): Flow<Either<QueryError, EventRevision>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr).mapLeft { DatabaseNotFound(databaseNameStr) }) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> {
                when (val subject = EventSubject(subjectStr)) {
                    is Either.Left -> emit(subject)
                    is Either.Right -> {
                        when (val database = repository.databaseAtRevision(databaseName.value, revision)) {
                            is Either.Left -> emit(database)
                            is Either.Right -> emitAll(database.value.subject(subject.value).map { it.right() })
                        }
                    }
                }
            }
        }
    }

    fun eventType(
        databaseNameStr: String,
        revision: DatabaseRevision,
        typeStr: String,
    ): Flow<Either<QueryError, EventRevision>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr).mapLeft { DatabaseNotFound(databaseNameStr) }) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> {
                when (val type = EventType(typeStr)) {
                    is Either.Left -> emit(type)
                    is Either.Right -> {
                        when (val database = repository.databaseAtRevision(databaseName.value, revision)) {
                            is Either.Left -> emit(database)
                            is Either.Right -> emitAll(database.value.eventType(type.value).map { it.right() })
                        }
                    }
                }
            }
        }
    }

    suspend fun eventById(
        databaseNameStr: String,
        revision: DatabaseRevision,
        streamNameStr: String,
        idStr: String,
    ): Either<QueryError, Event> = either {
        val databaseName = DatabaseName(databaseNameStr)
            .mapLeft { DatabaseNotFound(databaseNameStr) }
            .bind()
        val streamName = StreamName(streamNameStr).bind()
        val database = repository.databaseAtRevision(databaseName, revision).bind()
        val id = EventId(idStr).bind()
        database.eventById(streamName, id).bind()
    }

    fun eventsByRevision(
        databaseNameStr: String,
        revisions: List<EventRevision>,
    ): Flow<Either<QueryError, Event>> = flow {
        val database = either {
            val databaseName = DatabaseName(databaseNameStr)
                .mapLeft { DatabaseNotFound(databaseNameStr) }
                .bind()
            repository.latestDatabase(databaseName).bind()
        }
        when (database) {
            is Either.Left -> emit(database)
            is Either.Right -> emitAll(
                database.value.eventsByRevision(revisions)
            )
        }
    }
}