package com.evidentdb.server.service

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.raise.either
import arrow.core.raise.mapOrAccumulate
import com.evidentdb.server.adapter.EvidentDbAdapter
import com.evidentdb.server.domain_model.*
import com.evidentdb.server.transfer.toDomain
import com.evidentdb.server.transfer.toTransfer
import com.evidentdb.v1.proto.service.*
import io.cloudevents.protobuf.toDomain
import io.cloudevents.protobuf.toTransfer
import kotlinx.coroutines.flow.*
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
        LOGGER.info("createDatabase(databaseName: {})", request.databaseName)
        val reply = CreateDatabaseReply.newBuilder()
        when (val result = adapter.createDatabase(request.databaseName)) {
            is Either.Left -> {
                val exception = result.value.toStatusException()
                LOGGER.warn(
                    "createDatabase error in database {}: {}",
                    request.databaseName, exception.message
                )
                throw exception
            }
            is Either.Right -> {
                LOGGER.info("createDatabase success: {}", request.databaseName)
                reply.database = result.value.toTransfer()
            }
        }
        return reply.build()
    }

    override suspend fun transactBatch(request: TransactBatchRequest): TransactBatchReply {
        LOGGER.info("transactBatch(databaseName: {})", request.databaseName)
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
                    request.databaseName,
                    request.eventsList.map { ProposedEvent(it.toDomain()) },
                    constraints.bind(),
                ).bind()
            }
        when (result) {
            is Either.Left -> {
                val exception = result.value.toStatusException()
                LOGGER.warn(
                    "transactBatch error in database {}: {}",
                    request.databaseName, exception.message
                )
                throw exception
            }
            is Either.Right -> {
                LOGGER.info(
                    "transactBatch success in database {}, now at revision {}",
                    request.databaseName, result.value.revision
                )
                reply.batch = result.value.toTransfer()
            }
        }
        return reply.build()
    }

    override suspend fun deleteDatabase(request: DeleteDatabaseRequest): DeleteDatabaseReply {
        LOGGER.info("deleteDatabase(databaseName: {})", request.databaseName)
        val reply = DeleteDatabaseReply.newBuilder()
        when (val result = adapter.deleteDatabase(request.databaseName)) {
            is Either.Left -> {
                val exception = result.value.toStatusException()
                LOGGER.warn(
                    "deleteDatabase error in database {}: {}",
                    request.databaseName, exception.message
                )
                throw exception
            }
            is Either.Right -> {
                LOGGER.info("deleteDatabase success: {}", request.databaseName)
                reply.database = result.value.toTransfer()
            }
        }
        return reply.build()
    }

    // Query
    override fun fetchCatalog(request: CatalogRequest): Flow<CatalogReply> = flow {
        LOGGER.info("fetchCatalog()")
        emitAll(adapter.fetchCatalog().map {
            CatalogReply.newBuilder()
                .setDatabaseName(it.value)
                .build()
        })
    }

    override suspend fun fetchLatestDatabase(request: LatestDatabaseRequest): DatabaseReply {
        LOGGER.info("fetchLatestDatabase(databaseName: {})", request.databaseName)
        val reply = DatabaseReply.newBuilder()
        when (val result = adapter.fetchLatestDatabase(request.databaseName)) {
            is Either.Left -> throw result.value.toStatusException()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    // TODO: Write test to ensure that we await if database revision isn't already available
    override suspend fun awaitDatabase(
        request: AwaitDatabaseRequest
    ): DatabaseReply {
        LOGGER.info(
            "awaitDatabaseGreaterThanRevision(databaseName: {}, minimumRevision: {})",
            request.databaseName, request.atLeastRevision
        )
        val reply = DatabaseReply.newBuilder()
        when (val result = adapter.awaitDatabase(
            request.databaseName,
            request.atLeastRevision.toULong()
        )) {
            is Either.Left -> throw result.value.toStatusException()
            is Either.Right -> reply.database = result.value.toTransfer()
        }
        return reply.build()
    }

    override fun scanDatabaseLog(request: LogScanRequest): Flow<DatabaseLogReply> = flow {
        LOGGER.info(
            "scanDatabaseLog(databaseName: {}, startAtRevision: {}, includeEventDetail: {})",
            request.databaseName, request.startAtRevision, request.includeEventDetail
        )
        emitAll(
            if (request.includeEventDetail) {
                adapter.scanDatabaseLogDetail(
                    request.databaseName,
                    request.startAtRevision.toULong()
                ).map {
                    val reply = DatabaseLogReply.newBuilder()
                    when (it) {
                        is Either.Left -> throw it.value.toStatusException()
                        is Either.Right -> reply.detail = it.value.toTransfer()
                    }
                    reply.build()
                }
            } else {
                adapter.scanDatabaseLog(
                    request.databaseName,
                    request.startAtRevision.toULong()
                ).map {
                    val reply = DatabaseLogReply.newBuilder()
                    when (it) {
                        is Either.Left -> throw it.value.toStatusException()
                        is Either.Right -> reply.summary = it.value.toTransfer()
                    }
                     reply.build()
                }
            }
        )
    }

    override fun tailDatabaseLog(request: LogTailRequest): Flow<DatabaseLogReply> = flow {
        LOGGER.info(
            "tailDatabaseLog(databaseName: {}, includeEventDetail: {})",
            request.databaseName, request.includeEventDetail
        )
        emitAll(
            if (request.includeEventDetail) {
                adapter.tailDatabaseLogDetail(request.databaseName).map {
                    val reply = DatabaseLogReply.newBuilder()
                    when (it) {
                        is Either.Left -> throw it.value.toStatusException()
                        is Either.Right -> reply.detail = it.value.toTransfer()
                    }
                    reply.build()
                }
            } else {
                adapter.tailDatabaseLog(request.databaseName).map {
                    val reply = DatabaseLogReply.newBuilder()
                    when (it) {
                        is Either.Left -> throw it.value.toStatusException()
                        is Either.Right -> reply.summary = it.value.toTransfer()
                    }
                    reply.build()
                }
            }
        )
    }

    override fun queryEventIndex(request: EventIndexRequest): Flow<EventIndexReply> = flow {
        LOGGER.info(
            "queryEventIndex(databaseName: {}, revision: {}, includeEventDetail: {}, queryCase: {})",
            request.databaseName, request.revision, request.includeEventDetail, request.queryCase
        )
        if (request.includeEventDetail) {
            val eventIndex = when(request.queryCase) {
                EventIndexRequest.QueryCase.STREAM ->
                    adapter.fetchEventsByStream(
                        request.databaseName,
                        request.revision.toULong(),
                        request.stream.stream
                    )
                EventIndexRequest.QueryCase.SUBJECT_STREAM ->
                    adapter.fetchEventsBySubjectAndStream(
                        request.databaseName,
                        request.revision.toULong(),
                        request.subjectStream.stream,
                        request.subjectStream.subject
                    )
                EventIndexRequest.QueryCase.SUBJECT ->
                    adapter.fetchEventsBySubject(
                        request.databaseName,
                        request.revision.toULong(),
                        request.subject.subject
                    )
                EventIndexRequest.QueryCase.EVENT_TYPE ->
                    adapter.fetchEventsByType(
                        request.databaseName,
                        request.revision.toULong(),
                        request.eventType.eventType
                    )
                EventIndexRequest.QueryCase.EVENT_BY_ID ->
                    flowOf(
                        adapter.fetchEventById(
                            request.databaseName,
                            request.revision.toULong(),
                            request.eventById.stream,
                            request.eventById.eventId,
                        )
                    )
                EventIndexRequest.QueryCase.QUERY_NOT_SET, null ->
                    throw InvalidIndexQuery.toStatusException()
            }
            emitAll(
                eventIndex.map {
                    val reply = EventIndexReply.newBuilder()
                    when (it) {
                        is Either.Left -> throw it.value.toStatusException()
                        is Either.Right -> reply.detail = it.value.event.toTransfer()
                    }
                    reply.build()
                }
            )
        } else {
            val eventIndex = when(request.queryCase) {
                EventIndexRequest.QueryCase.STREAM ->
                    adapter.fetchEventRevisionsByStream(
                        request.databaseName,
                        request.revision.toULong(),
                        request.stream.stream
                    )
                EventIndexRequest.QueryCase.SUBJECT_STREAM ->
                    adapter.fetchEventRevisionsBySubjectAndStream(
                        request.databaseName,
                        request.revision.toULong(),
                        request.subjectStream.stream,
                        request.subjectStream.subject
                    )
                EventIndexRequest.QueryCase.SUBJECT ->
                    adapter.fetchEventRevisionsBySubject(
                        request.databaseName,
                        request.revision.toULong(),
                        request.subject.subject
                    )
                EventIndexRequest.QueryCase.EVENT_TYPE ->
                    adapter.fetchEventRevisionsByType(
                        request.databaseName,
                        request.revision.toULong(),
                        request.eventType.eventType
                    )
                EventIndexRequest.QueryCase.EVENT_BY_ID ->
                    flowOf(
                        adapter.fetchEventById(
                            request.databaseName,
                            request.revision.toULong(),
                            request.eventById.stream,
                            request.eventById.eventId,
                        ).map { it.revision }
                    )
                EventIndexRequest.QueryCase.QUERY_NOT_SET, null ->
                    throw InvalidIndexQuery.toStatusException()
            }
            emitAll(eventIndex.map {
                val reply = EventIndexReply.newBuilder()
                when (it) {
                    is Either.Left -> throw it.value.toStatusException()
                    is Either.Right -> reply.revision = it.value.toLong()
                }
                reply.build()
            })
        }
    }

    override fun fetchEventsByRevisions(request: EventsByRevisionsRequest): Flow<EventReply> = flow {
        LOGGER.info(
            "fetchEventsByRevisions(databaseName: {}, eventRevisions: {})",
            request.databaseName, request.eventRevisionsList
        )
        emitAll(adapter.fetchEventsByRevisions(
            request.databaseName, request.eventRevisionsList.map { it.toULong() }
        ).map {
            val reply = EventReply.newBuilder()
            when (it) {
                is Either.Left -> throw it.value.toStatusException()
                is Either.Right -> reply.event = it.value.toTransfer()
            }
            reply.build()
        })
    }
}