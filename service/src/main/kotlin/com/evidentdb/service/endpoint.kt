package com.evidentdb.service

import arrow.core.Either
import com.evidentdb.domain.*
import com.evidentdb.dto.protobuf.toProto
import com.evidentdb.dto.protobuf.unvalidatedProposedEventFromProto
import com.evidentdb.dto.v1.proto.BatchProposal
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo
import com.evidentdb.service.v1.*
import io.cloudevents.protobuf.toProto
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EvidentDbEndpoint(
    private val commandService: CommandService,
    private val queryService: QueryService,
    private val databaseChannel: Channel<Database>,
) : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EvidentDbEndpoint::class.java)
    }

    override suspend fun createDatabase(request: DatabaseCreationInfo): CreateDatabaseReply {
        LOGGER.debug("createDatabase request received: $request")
        val builder = CreateDatabaseReply.newBuilder()
        commandService.createDatabase(request.name).bimap(
            {
                when(it) {
                    is DatabaseNameAlreadyExistsError -> {
                        builder.databaseNameAlreadyExistsError = it.toProto()
                    }
                    is InvalidDatabaseNameError -> {
                        builder.invalidDatabaseNameError = it.toProto()
                    }
                    is InternalServerError -> {
                        builder.internalServerError = it.toProto()
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
                        is DatabaseNotFoundError -> {
                            builder.databaseNotFoundError = it.toProto()
                        }
                        is InternalServerError -> {
                            builder.internalServerError = it.toProto()
                        }
                        is InvalidDatabaseNameError -> {
                            builder.invalidDatabaseNameError = it.toProto()
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
                        is InvalidDatabaseNameError -> {
                            builder.invalidDatabaseNameError = it.toProto()
                        }
                        is DatabaseNotFoundError -> {
                            builder.databaseNotFoundError = it.toProto()
                        }
                        is InvalidEventsError -> {
                            builder.invalidEventsError = it.toProto()
                        }
                        NoEventsProvidedError -> {
                            builder.noEventsProvidedErrorBuilder.build()
                        }
                        is DuplicateBatchError -> {
                            builder.duplicateBatchError = it.toProto()
                        }
                        is StreamStateConflictsError -> {
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

    override suspend fun catalog(request: CatalogRequest): CatalogReply {
        LOGGER.debug("catalog request received: $request")
        val builder = CatalogReply.newBuilder()
        builder.addAllDatabases(queryService.getCatalog().map { it.toProto() })
        return builder.build()
    }

    override fun connect(request: DatabaseRequest): Flow<DatabaseReply> = flow {
        LOGGER.debug("connect request received: $request")
        val builder = DatabaseReply.newBuilder()
        when(val result = queryService.getDatabase(request.name)) {
            is Either.Left -> builder.notFoundBuilder.name = result.value.name
            is Either.Right -> builder.database = result.value.toProto()
        }
        emit(builder.build())

        databaseChannel.consumeEach { database ->
            val replyBuilder = DatabaseReply.newBuilder()
            replyBuilder.database = database.toProto()
            emit(replyBuilder.build())
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

    override fun databaseLog(request: DatabaseLogRequest): Flow<DatabaseLogReply> = flow {
        LOGGER.debug("databaseLog request received: $request")
        when (val result = queryService.getDatabaseLog(request.database)) {
            is Either.Left -> {
                val builder = DatabaseLogReply.newBuilder()
                builder.databaseNotFoundBuilder.name = result.value.name
                emit(builder.build())
            }
            is Either.Right ->
                for (batch in result.value) {
                    val builder = DatabaseLogReply.newBuilder()
                    builder.batch = batch.toProto()
                    emit(builder.build())
                }
        }
    }

    override fun stream(request: StreamRequest): Flow<StreamEventIdReply> = flow {
        LOGGER.debug("stream request received: $request")
        when (val result = queryService.getStream(
            request.database,
            request.databaseRevision,
            request.stream,
        )
        ) {
            is Either.Left -> {
                val builder = StreamEventIdReply.newBuilder()
                builder.streamNotFoundBuilder
                    .setDatabase(result.value.database)
                    .setStream(result.value.stream)
                emit(builder.build())
            }
            is Either.Right ->
                for (eventId in result.value.eventIds) {
                    val builder = StreamEventIdReply.newBuilder()
                        .setEventId(eventId.toString())
                    emit(builder.build())
                }
        }
    }

    override fun subjectStream(request: SubjectStreamRequest): Flow<StreamEventIdReply> = flow {
        TODO("Not Implemented Yet")
    }

    override fun events(requests: Flow<EventRequest>): Flow<EventReply> = flow {
        requests.collect { request ->
            LOGGER.debug("event request received: $request")
            val eventId = EventId.fromString(request.eventId)
            val replyBuilder = EventReply.newBuilder()
            when (val result = queryService.getEvents(request.database, eventId)) {
                is Either.Left -> {
                    replyBuilder.eventNotFoundBuilder
                        .setDatabase(result.value.database)
                        .setEventId(result.value.eventId.toString())
                }

                is Either.Right -> {
                    replyBuilder.eventMapEntryBuilder.eventId = result.value.first.toString()
                    replyBuilder.eventMapEntryBuilder.event = result.value.second.toProto()
                }
            }
            emit(replyBuilder.build())
        }
    }
}