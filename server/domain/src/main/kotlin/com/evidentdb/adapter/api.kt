package com.evidentdb.adapter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.resilience.Schedule
import com.evidentdb.application.CommandService
import com.evidentdb.application.DatabaseRepository
import com.evidentdb.domain_model.*
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private fun isNotWriteCollision(transactionResult: Either<EvidentDbError, Database>) = when (transactionResult) {
    is Either.Left -> transactionResult.value !is WriteCollisionError
    is Either.Right -> true
}

private val batchTransactRetrySchedule = Schedule
    .exponential<Either<EvidentDbError, Database>>(10.milliseconds)
    .doUntil { result, duration -> duration > 60.seconds || isNotWriteCollision(result) }

interface EvidentDbAdapter {
    val commandService: CommandService
    val repository: DatabaseRepository

    // Command API
    suspend fun createDatabase(nameStr: String): Either<EvidentDbError, Database> =
        commandService.createDatabase(nameStr)

    /**
     * This function retries per `batchTransactRetrySchedule` in the event of write collision
     */
    suspend fun transactBatch(
        databaseNameStr: String,
        events: List<CloudEvent>,
        constraints: List<BatchConstraint>
    ): Either<EvidentDbError, Database> {
        var result: Either<EvidentDbError, Database> = WriteCollisionError(0uL, 0uL).left()
        batchTransactRetrySchedule.repeat {
            result = commandService.transactBatch(databaseNameStr, events.map { ProposedEvent(it) }, constraints)
            result
        }
        return result
    }

    suspend fun deleteDatabase(nameStr: String): Either<EvidentDbError, Database> =
        commandService.deleteDatabase(nameStr)

    // Query API
    suspend fun catalog(): Flow<Database> = repository.databaseCatalog()

    fun connect(databaseNameStr: String): Flow<Either<QueryError, Database>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr)) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> when (val database = repository.latestDatabase(databaseName.value)) {
                is Either.Left -> emit(database)
                is Either.Right -> emitAll(repository.subscribe(database.value.name))
            }
        }
    }

    suspend fun latestDatabase(databaseNameStr: String): Either<QueryError, Database> = either {
        val databaseName = DatabaseName(databaseNameStr).bind()
        repository.latestDatabase(databaseName).bind()
    }

    suspend fun databaseAtRevision(
        databaseNameStr: String,
        revision: DatabaseRevision,
    ): Either<QueryError, Database> = either {
        val databaseName = DatabaseName(databaseNameStr).bind()
        repository.databaseAtRevision(databaseName, revision).bind()
    }

    fun databaseLog(
        databaseNameStr: String,
        revision: DatabaseRevision,
    ): Flow<Either<QueryError, Batch>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr)) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> {
                when (val database = repository.databaseAtRevision(databaseName.value, revision)) {
                    is Either.Left -> emit(database)
                    is Either.Right -> emitAll(database.value.log().map { it.right() })
                }
            }
        }
    }

    suspend fun eventById(
        databaseNameStr: String,
        streamNameStr: String,
        revision: DatabaseRevision,
        idStr: String,
    ): Either<QueryError, Event> = either {
        val databaseName = DatabaseName(databaseNameStr).bind()
        val streamName = StreamName(streamNameStr).bind()
        val database = repository.databaseAtRevision(databaseName, revision).bind()
        val id = EventId(idStr).bind()
        database.eventById(streamName, id).bind()
    }

    fun stream(
        databaseNameStr: String,
        revision: DatabaseRevision,
        streamNameStr: String,
    ): Flow<Either<QueryError, EventRevision>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr)) {
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
        when (val databaseName = DatabaseName(databaseNameStr)) {
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
        when (val databaseName = DatabaseName(databaseNameStr)) {
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
        when (val databaseName = DatabaseName(databaseNameStr)) {
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

    fun eventsByRevision(
        databaseNameStr: String,
        revisions: Flow<EventRevision>,
    ): Flow<Either<QueryError, Event>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr)) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> emitAll(
                repository
                    .eventsByRevision(databaseName.value, revisions)
                    .map { it.right() }
            )
        }
    }
}