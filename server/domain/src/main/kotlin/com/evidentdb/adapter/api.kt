package com.evidentdb.adapter

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.right
import com.evidentdb.application.CommandService
import com.evidentdb.application.DatabaseRepository
import com.evidentdb.domain_model.*
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

interface EvidentDbAdapter {
    val commandService: CommandService
    val repository: DatabaseRepository

    // Command API
    suspend fun createDatabase(nameStr: String): Either<EvidentDbError, Database> =
        commandService.createDatabase(nameStr)
    suspend fun transactBatch(
        databaseNameStr: String,
        events: List<CloudEvent>,
        constraints: List<BatchConstraint>
    ): Either<EvidentDbError, Database> =
        commandService.transactBatch(databaseNameStr, events.map { ProposedEvent(it) }, constraints)
    suspend fun deleteDatabase(nameStr: String): Either<EvidentDbError, Database> =
        commandService.deleteDatabase(nameStr)

    // Query API
    suspend fun catalog(): Flow<Database> = repository.databaseCatalog()

    fun connect(databaseNameStr: String): Flow<Either<QueryError, Database>> = flow {
        when (val databaseName = DatabaseName(databaseNameStr)) {
            is Either.Left -> emit(databaseName)
            is Either.Right -> emitAll(repository.subscribe(databaseName.value))
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
        revision: DatabaseRevision,
        idStr: String,
    ): Either<QueryError, CloudEvent> = either {
        val databaseName = DatabaseName(databaseNameStr).bind()
        val database = repository.databaseAtRevision(databaseName, revision).bind()
        val id = EventId(idStr).bind()
        database.eventById(id).bind()
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
    ): Flow<Either<QueryError, CloudEvent>> = flow {
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