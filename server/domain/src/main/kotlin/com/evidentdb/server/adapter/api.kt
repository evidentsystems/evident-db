package com.evidentdb.server.adapter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.resilience.Schedule
import com.evidentdb.server.application.CommandService
import com.evidentdb.server.application.DatabaseRepository
import com.evidentdb.server.application.DatabaseUpdateStream
import com.evidentdb.server.application.Lifecycle
import com.evidentdb.server.domain_model.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private val LOGGER = LoggerFactory.getLogger("com.evidentdb.server.adapter.ApiKt")

private fun isNotWriteCollision(transactionResult: Either<EvidentDbCommandError, IndexedBatch>) =
    when (transactionResult) {
        is Either.Left -> transactionResult.value !is ConcurrentWriteCollision
        is Either.Right -> true
    }

private val batchTransactRetrySchedule = Schedule
    .exponential<Either<EvidentDbCommandError, IndexedBatch>>(10.milliseconds)
    .doUntil { result, duration ->
        val timedOut = duration > 60.seconds
        val notWriteCollision = isNotWriteCollision(result)
        if (timedOut) {
            LOGGER.warn("Write collision retries timed out after duration {}, won't retry", duration)
        } else if (notWriteCollision) {
            LOGGER.info("Encountered a non-write-collision condition after duration {}", duration)
        } else {
            LOGGER.info("Encountered a write collision after duration {}, will retry...", duration)
        }
        timedOut || notWriteCollision
    }

interface EvidentDbAdapter: Lifecycle {
    val commandService: CommandService
    val repository: DatabaseRepository
    val databaseUpdateStream: DatabaseUpdateStream

    // Lifecycle
    override fun setup() {
        commandService.setup()
        repository.setup()
        databaseUpdateStream.setup()
    }

    override fun teardown() {
        commandService.teardown()
        repository.teardown()
        databaseUpdateStream.teardown()
    }

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
    ): Either<EvidentDbCommandError, IndexedBatch> {
        var result: Either<EvidentDbCommandError, IndexedBatch> =
            ConcurrentWriteCollision(0uL, 0uL).left()
        LOGGER.info("${javaClass}.transactBatch($databaseNameStr)")
        batchTransactRetrySchedule.repeat {
            result = commandService.transactBatch(databaseNameStr, events, constraints)
            if (result.isLeft()) {
                LOGGER.warn("Failure in transactBatch, may retry...")
            } else {
                LOGGER.info("Success in transactBatch, won't retry.")
            }
            result
        }
        return result
    }

    suspend fun deleteDatabase(nameStr: String): Either<EvidentDbCommandError, Database> =
        commandService.deleteDatabase(nameStr)

    // Query API
    suspend fun catalog(): Flow<DatabaseName> = repository.databaseCatalog()

    fun connect(databaseNameStr: String): Flow<Either<DatabaseNotFound, Database>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr).mapLeft { DatabaseNotFound(databaseNameStr) }) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> when (val database = repository.latestDatabase(databaseName.value)) {
                is Either.Left -> emit(database)
                is Either.Right -> {
                    emit(database)
                    emitAll(databaseUpdateStream.subscribe(database.value.name))
                }
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
        revision: Revision,
    ): Either<QueryError, Database> = either {
        val databaseName = DatabaseName(databaseNameStr)
            .mapLeft { DatabaseNotFound(databaseNameStr) }
            .bind()
        repository.databaseAtRevision(databaseName, revision).bind()
    }

    fun databaseLog(
        databaseNameStr: String,
        revision: Revision,
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
        revision: Revision,
        streamNameStr: String,
    ): Flow<Either<QueryError, Revision>> = flow {
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
        revision: Revision,
        streamNameStr: String,
        subjectStr: String,
    ): Flow<Either<QueryError, Revision>> = flow {
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
        revision: Revision,
        subjectStr: String,
    ): Flow<Either<QueryError, Revision>> = flow {
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
        revision: Revision,
        typeStr: String,
    ): Flow<Either<QueryError, Revision>> = flow {
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
        revision: Revision,
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
        revisions: List<Revision>,
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
