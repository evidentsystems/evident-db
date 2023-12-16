package com.evidentdb.service

import arrow.core.Either
import com.evidentdb.application.CommandService
import com.evidentdb.application.QueryService
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import com.evidentdb.dto.protobuf.toProto
import com.evidentdb.dto.protobuf.unvalidatedProposedEventFromProto
import com.evidentdb.dto.v1.proto.BatchProposal
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo
import com.evidentdb.service.v1.*
import io.cloudevents.protobuf.toProto
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EvidentDbEndpoint(
    private val commandService: CommandService,
    private val queryService: QueryService,
    private val databaseFlow: SharedFlow<ActiveDatabaseCommandModel>,
) : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EvidentDbEndpoint::class.java)
    }

    override suspend fun createDatabase(request: CreateDatabaseRequest): CreateDatabaseReply {
        LOGGER.debug("createDatabase request received: $request")
        val builder = CreateDatabaseReply.newBuilder()
        commandService.createDatabase(request.name).bimap(
            {
                when(it) {
                    is DatabaseNameAlreadyExists -> {
                        builder.databaseNameAlreadyExistsError = it.toProto()
                    }
                    is InvalidDatabaseName -> {
                        builder.invalidDatabaseNameError = it.toProto()
                    }
                    is InternalServerError -> {
                        builder.internalServerError = it.toProto()
                    }

                    is IllegalDatabaseCreationState -> {
                        builder.databaseTopicCreationError = it.toProto()
                    }
                }
            },
            {
                builder.databaseCreation = it.data.toProto()
            }
        )
        return builder.build()
    }

    override suspend fun deleteDatabase(request: DatabaseDeletionInfo): DeleteDatabaseReply {
        LOGGER.debug("deleteDatabase request received: $request")
        val builder = DeleteDatabaseReply.newBuilder()
        commandService.deleteDatabase(request.name)
            .bimap(
                {
                    when(it) {
                        is DatabaseNotFound -> {
                            builder.databaseNotFoundError = it.toProto()
                        }
                        is InvalidDatabaseName -> {
                            builder.invalidDatabaseNameError = it.toProto()
                        }

                        is IllegalDatabaseDeletionState -> {
                            builder.databaseTopicDeletionError = it.toProto()
                        }

                        is InternalServerError -> {
                            builder.internalServerError = it.toProto()
                        }
                    }
                },
                {
                    builder.databaseDeletion = it.data.toProto()
                }
            )
        return builder.build()
    }

    override suspend fun transactBatch(request: BatchProposal): TransactBatchReply {
        LOGGER.debug("transactBatch request received: $request")
        val builder = TransactBatchReply.newBuilder()
        commandService.transactBatch(
            request.database,
            request.eventsList.map(::unvalidatedProposedEventFromProto)
        )
            .bimap(
                {
                    when(it) {
                        is InvalidDatabaseName -> {
                            builder.invalidDatabaseNameError = it.toProto()
                        }
                        is DatabaseNotFound -> {
                            builder.databaseNotFoundError = it.toProto()
                        }
                        is InvalidEvents -> {
                            builder.invalidEventsError = it.toProto()
                        }
                        EmptyBatch -> {
                            builder.noEventsProvidedErrorBuilder.build()
                        }
                        is DuplicateBatchError -> {
                            builder.duplicateBatchError = it.toProto()
                        }
                        is BatchConstraintViolations -> {
                            builder.streamStateConflictError = it.toProto()
                        }
                        is InternalServerError -> {
                            builder.internalServerError = it.toProto()
                        }
                    }
                },
                {
                    builder.batchTransaction = it.data.toProto()
                }
            )
        return builder.build()
    }

    override fun catalog(request: CatalogRequest): Flow<CatalogReply> {
        LOGGER.debug("catalog request received: $request")
        return queryService.getCatalog().map {
            val builder = CatalogReply.newBuilder()
                .setDatabase(it.toProto())
            builder.build()
        }
    }

    override fun connect(request: DatabaseRequest): Flow<DatabaseReply> = flow {
        LOGGER.debug("connect request received: $request")
        val initialReplyBuilder = DatabaseReply.newBuilder()
        when(val result = queryService.getDatabase(request.name)) {
            is Either.Left -> {
                initialReplyBuilder.notFoundBuilder.name = result.value.name
                emit(initialReplyBuilder.build())
            }
            is Either.Right -> {
                initialReplyBuilder.database = result.value.toProto()
                emit(initialReplyBuilder.build())

                databaseFlow
                    .filter { database -> request.name == database.name.value }
                    .collect { database ->
                        val replyBuilder = DatabaseReply.newBuilder()
                        replyBuilder.database = database.toProto()
                        emit(replyBuilder.build())
                    }
            }
        }
    }

    override suspend fun database(request: DatabaseRequest): DatabaseReply {
        LOGGER.debug("database request received: $request")
        val builder = DatabaseReply.newBuilder()
        when (val result = queryService.getDatabase(
            request.name,
            request.revision
        )) {
            is Either.Left -> builder.notFoundBuilder.name = request.name
            is Either.Right -> builder.database = result.value.toProto()
        }

        return builder.build()
    }

    override fun databaseLog(
        request: DatabaseLogRequest
    ): Flow<DatabaseLogReply> = flow {
        LOGGER.debug("databaseLog request received: $request")
        when (val result = queryService.getDatabaseLog(request.database)) {
            is Either.Left -> {
                val builder = DatabaseLogReply.newBuilder()
                builder.databaseNotFoundBuilder.name = result.value.name
                emit(builder.build())
            }
            is Either.Right ->
                result.value.collect { batch ->
                    val builder = DatabaseLogReply.newBuilder()
                    builder.batch = batch.toProto()
                    emit(builder.build())
                }
        }
    }

    override fun stream(request: StreamRequest): Flow<StreamEntryReply> = flow {
        LOGGER.debug("stream request received: $request")
        when (val result = queryService.getStream(
            request.database,
            request.databaseRevision,
            request.stream,
        )
        ) {
            is Either.Left -> {
                val builder = StreamEntryReply.newBuilder()
                builder.streamNotFoundBuilder
                    .setDatabase(result.value.database)
                    .setStream(result.value.stream)
                emit(builder.build())
            }
            is Either.Right ->
                result.value.collect { (streamRevision, eventId) ->
                    val builder = StreamEntryReply.newBuilder()
                        .setStreamMapEntry(
                            StreamMapEntry.newBuilder()
                                .setStreamRevision(streamRevision)
                                .setEventId(eventId)
                        )
                    emit(builder.build())
                }
        }
    }

    override fun subjectStream(request: SubjectStreamRequest): Flow<StreamEntryReply> = flow {
        LOGGER.debug("subjectStream request received: $request")
        when (val result = queryService.getSubjectStream(
            request.database,
            request.databaseRevision,
            request.stream,
            request.subject
        )) {
            is Either.Left -> {
                // TODO: New SubjectStreamNotFound error type
                val builder = StreamEntryReply.newBuilder()
                builder.streamNotFoundBuilder
                    .setDatabase(result.value.database)
                    .setStream(result.value.stream)
                emit(builder.build())
            }
            is Either.Right ->
                result.value.collect { (streamRevision, eventId) ->
                    val builder = StreamEntryReply.newBuilder()
                        .setStreamMapEntry(
                            StreamMapEntry.newBuilder()
                                .setStreamRevision(streamRevision)
                                .setEventId(eventId)
                        )
                    emit(builder.build())
                }
        }
    }

    override fun events(requests: Flow<EventRequest>): Flow<EventReply> = flow {
        requests.collect { request ->
            LOGGER.debug("event request received: $request")
            val eventId = request.eventId
            val replyBuilder = EventReply.newBuilder()
            when (val result = queryService.getEvent(request.database, eventId)) {
                is Either.Left -> {
                    replyBuilder.eventNotFoundBuilder
                        .setDatabase(result.value.database)
                        .setEventId(result.value.eventIndex)
                }

                is Either.Right -> {
                    replyBuilder.eventMapEntryBuilder.eventId = result.value.first
                    replyBuilder.eventMapEntryBuilder.event = result.value.second.toProto()
                }
            }
            emit(replyBuilder.build())
        }
    }
}