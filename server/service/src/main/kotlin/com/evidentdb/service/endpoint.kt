package com.evidentdb.service

import arrow.core.Either
import com.evidentdb.adapter.EvidentDbAdapter
import com.evidentdb.domain_model.*
import com.evidentdb.service.v1.*
import com.evidentsystems.transfer.toDomain
import com.evidentsystems.transfer.toTransfer
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
        val reply = CreateDatabaseReply.newBuilder()
        when (val result = adapter.createDatabase(request.name)) {
            is Either.Left -> reply.error = result.value.toTransfer()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    override suspend fun transactBatch(request: TransactBatchRequest): TransactBatchReply {
        val reply = TransactBatchReply.newBuilder()
        when (val result = adapter.transactBatch(
                request.database,
                request.eventsList.map { ProposedEvent(it.toDomain()) },
                request.constraintsList.map { it.toDomain() }
        )) {
            is Either.Left -> reply.error = result.value.toTransfer()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    override suspend fun deleteDatabase(request: DeleteDatabaseRequest): DeleteDatabaseReply {
        val reply = DeleteDatabaseReply.newBuilder()
        when (val result = adapter.deleteDatabase(request.name)) {
            is Either.Left -> reply.error = result.value.toTransfer()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    // Query
    override fun catalog(request: CatalogRequest): Flow<CatalogReply> = flow {
        emitAll(adapter.catalog().map {
            CatalogReply.newBuilder()
                .setDatabase(it.toTransfer())
                .build()
        })
    }

    override fun connect(request: ConnectRequest): Flow<DatabaseReply> = flow {
        emitAll(adapter.connect(request.name).map {
            val reply = DatabaseReply.newBuilder()
            when (it) {
                is Either.Left -> reply.error = it.value.toTransfer()
                is Either.Right -> reply.database = it.value.toTransfer()
            }
            reply.build()
        })
    }

    override suspend fun latestDatabase(request: LatestDatabaseRequest): DatabaseReply {
        val reply = DatabaseReply.newBuilder()
        when (val result = adapter.latestDatabase(request.name)) {
            is Either.Left -> reply.error = result.value.toTransfer()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    override suspend fun databaseAtRevision(request: DatabaseAtRevisionRequest): DatabaseReply {
        val reply = DatabaseReply.newBuilder()
        when (val result = adapter.databaseAtRevision(request.name, request.revision.toULong())) {
            is Either.Left -> reply.error = result.value.toTransfer()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    override fun databaseLog(request: DatabaseLogRequest): Flow<DatabaseLogReply> = flow {
        emitAll(adapter.databaseLog(request.name, request.revision.toULong()).map {
            val reply = DatabaseLogReply.newBuilder()
            when (it) {
                is Either.Left -> reply.error = it.value.toTransfer()
                is Either.Right -> reply.batch = it.value.toTransfer()
            }
            reply.build()
        })
    }

    override fun stream(request: StreamRequest): Flow<EventRevisionReply> = flow {
        emitAll(adapter.stream(
            request.database,
            request.revision.toULong(),
            request.stream
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> reply.error = it.value.toTransfer()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override fun subjectStream(request: SubjectStreamRequest): Flow<EventRevisionReply> = flow {
        emitAll(adapter.subjectStream(
            request.database,
            request.revision.toULong(),
            request.stream,
            request.subject
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> reply.error = it.value.toTransfer()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override fun subject(request: SubjectRequest): Flow<EventRevisionReply> = flow {
        emitAll(adapter.subject(
            request.database,
            request.revision.toULong(),
            request.subject
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> reply.error = it.value.toTransfer()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override fun eventType(request: EventTypeRequest): Flow<EventRevisionReply> = flow {
        emitAll(adapter.eventType(
            request.database,
            request.revision.toULong(),
            request.eventType
        ).map {
            val reply = EventRevisionReply.newBuilder()
            when (it) {
                is Either.Left -> reply.error = it.value.toTransfer()
                is Either.Right -> reply.revision = it.value.toLong()
            }
            reply.build()
        })
    }

    override suspend fun eventById(request: EventByIdRequest): EventReply {
        val reply = EventReply.newBuilder()
        when (val result = adapter.eventById(
            request.database,
            request.revision.toULong(),
            request.stream,
            request.eventId
        )) {
            is Either.Left -> reply.error = result.value.toTransfer()
            is Either.Right -> reply.event = result.value.toTransfer()
        }
        return reply.build()
    }

    override fun events(request: EventByRevisionRequest): Flow<EventReply> = flow {
        emitAll(adapter.eventsByRevision(
            request.database, request.eventRevisionsList.map { it.toULong() }
        ).map {
            val reply = EventReply.newBuilder()
            when (it) {
                is Either.Left -> reply.error = it.value.toTransfer()
                is Either.Right -> reply.event = it.value.toTransfer()
            }
            reply.build()
        })
    }
}