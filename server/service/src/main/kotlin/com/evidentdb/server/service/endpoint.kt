package com.evidentdb.server.service

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.raise.either
import arrow.core.raise.mapOrAccumulate
import com.evidentdb.server.adapter.EvidentDbAdapter
import com.evidentdb.server.domain_model.*
import com.evidentdb.server.transfer.toDomain
import com.evidentdb.server.transfer.toTransfer
import com.evidentdb.service.v1.*
import io.cloudevents.protobuf.toDomain
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EvidentDbEndpoint(
        private val adapter: EvidentDbAdapter
) : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EvidentDbEndpoint::class.java)
    }

    // Command
    override suspend fun createDatabase(request: CreateDatabaseRequest): CreateDatabaseReply {
        LOGGER.info("createDatabase: {}", request.name)
        val reply = CreateDatabaseReply.newBuilder()
        when (val result = adapter.createDatabase(request.name)) {
            is Either.Left -> {
                LOGGER.warn("createDatabase error: {}", request.name)
                throw result.value.toRuntimeException()
            }
            is Either.Right -> {
                LOGGER.info("createDatabase success: {}", request.name)
                reply.database = result.value.toTransfer()
            }
        }
        return reply.build()
    }

    override suspend fun transactBatch(request: TransactBatchRequest): TransactBatchReply {
        LOGGER.info("transactBatch in database: {}", request.database)
        val reply = TransactBatchReply.newBuilder()
        val constraints = either<NonEmptyList<InvalidBatchConstraint>, List<BatchConstraint>> {
            mapOrAccumulate(
                request.constraintsList.withIndex()
            ) { indexedConstraint ->
                indexedConstraint.value.toDomain()
                    .mapLeft { errs ->
                        InvalidBatchConstraint(indexedConstraint.index, errs)
                    }.bind()
            }
        }.mapLeft { InvalidBatchConstraints(it) }
        val result: Either<EvidentDbCommandError, IndexedBatch> =
            either {
                adapter.transactBatch(
                    request.database,
                    request.eventsList.map { ProposedEvent(it.toDomain()) },
                    constraints.bind(),
                ).bind()
            }
        when (result) {
            is Either.Left -> {
                val exception = result.value.toRuntimeException()
                LOGGER.warn(
                    "transactBatch error in database {}: {}",
                    request.database, exception.message
                )
                throw exception
            }
            is Either.Right -> {
                LOGGER.info("transactBatch success in database {}", request.database)
                reply.batch = result.value.toTransfer()
            }
        }
        return reply.build()
    }

    override suspend fun deleteDatabase(request: DeleteDatabaseRequest): DeleteDatabaseReply {
        LOGGER.info("deleteDatabase: {}", request.name)
        val reply = DeleteDatabaseReply.newBuilder()
        when (val result = adapter.deleteDatabase(request.name)) {
            is Either.Left -> {
                LOGGER.warn("deleteDatabase error: {}", request.name)
                throw result.value.toRuntimeException()
            }
            is Either.Right -> {
                LOGGER.info("deleteDatabase success: {}", request.name)
                reply.database = result.value.toTransfer()
            }
        }
        return reply.build()
    }

    // Query
    override fun catalog(request: CatalogRequest): Flow<CatalogReply> = flow {
        LOGGER.info("catalog")
        emitAll(adapter.catalog().map {
            CatalogReply.newBuilder()
                .setDatabaseName(it.value)
                .build()
        })
    }

    override fun connect(request: ConnectRequest): Flow<DatabaseReply> = flow {
        LOGGER.info("connect: {}", request.name)
        emitAll(adapter.connect(request.name).map {
            val reply = DatabaseReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toRuntimeException()
                is Either.Right -> reply.database = it.value.toTransfer()
            }
            reply.build()
        })
    }

    override suspend fun latestDatabase(request: LatestDatabaseRequest): DatabaseReply {
        LOGGER.info("latestDatabase: {}", request.name)
        val reply = DatabaseReply.newBuilder()
        when (val result = adapter.latestDatabase(request.name)) {
            is Either.Left -> throw result.value.toRuntimeException()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    override suspend fun databaseAtRevision(request: DatabaseAtRevisionRequest): DatabaseReply {
        LOGGER.info("databaseAtRevision: {}, {}", request.name, request.revision)
        val reply = DatabaseReply.newBuilder()
        when (val result = adapter.databaseAtRevision(request.name, request.revision.toULong())) {
            is Either.Left -> throw result.value.toRuntimeException()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    override fun databaseLog(request: DatabaseLogRequest): Flow<DatabaseLogReply> = flow {
        LOGGER.info("databaseLog: {}, {}", request.name, request.revision)
        emitAll(adapter.databaseLog(request.name, request.revision.toULong()).map {
            val reply = DatabaseLogReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toRuntimeException()
                is Either.Right -> reply.batch = it.value.toTransfer()
            }
            reply.build()
        })
    }

    override fun stream(request: StreamRequest): Flow<EventRevisionReply> = flow {
        LOGGER.info("stream: {}, {}, {}", request.database, request.revision, request.stream)
        emitAll(adapter.stream(
            request.database,
            request.revision.toULong(),
            request.stream
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toRuntimeException()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override fun subjectStream(request: SubjectStreamRequest): Flow<EventRevisionReply> = flow {
        LOGGER.info(
            "subjectStream: {}, {}, {}, {}",
            request.database, request.revision, request.stream, request.subject
        )
        emitAll(adapter.subjectStream(
            request.database,
            request.revision.toULong(),
            request.stream,
            request.subject
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toRuntimeException()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override fun subject(request: SubjectRequest): Flow<EventRevisionReply> = flow {
        LOGGER.info(
            "subject: {}, {}, {}",
            request.database, request.revision, request.subject
        )
        emitAll(adapter.subject(
            request.database,
            request.revision.toULong(),
            request.subject
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toRuntimeException()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override fun eventType(request: EventTypeRequest): Flow<EventRevisionReply> = flow {
        LOGGER.info(
            "eventType: {}, {}, {}",
            request.database, request.revision, request.eventType
        )
        emitAll(adapter.eventType(
            request.database,
            request.revision.toULong(),
            request.eventType
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toRuntimeException()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override suspend fun eventById(request: EventByIdRequest): EventReply {
        LOGGER.info(
            "eventById: {}, {}, {}, {}",
            request.database, request.revision, request.stream, request.eventId
        )
        val reply = EventReply.newBuilder()
        when (val result = adapter.eventById(
            request.database,
            request.revision.toULong(),
            request.stream,
            request.eventId
        )) {
            is Either.Left -> throw result.value.toRuntimeException()
            is Either.Right -> reply.event = result.value.toTransfer()
        }
        return reply.build()
    }

    override fun events(request: EventByRevisionRequest): Flow<EventReply> = flow {
        LOGGER.info(
            "events: {}, {}",
            request.database, request.eventRevisionsList
        )
        emitAll(adapter.eventsByRevision(
            request.database, request.eventRevisionsList.map { it.toULong() }
        ).map {
            val reply = EventReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toRuntimeException()
                is Either.Right -> reply.event = it.value.toTransfer()
            }
            reply.build()
        })
    }
}