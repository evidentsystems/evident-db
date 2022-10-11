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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EvidentDbEndpoint(
    private val commandService: CommandService,
    private val queryService: QueryService,
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

    override suspend fun getCatalog(request: CatalogRequest): CatalogReply {
        LOGGER.debug("getCatalog request received: $request")
        val builder = CatalogReply.newBuilder()
        builder.addAllDatabases(queryService.getCatalog().map { it.toProto() })
        return builder.build()
    }

    override suspend fun getDatabase(request: DatabaseRequest): DatabaseReply {
        LOGGER.debug("getDatabase request received: $request")
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

    override suspend fun getDatabaseLog(request: DatabaseLogRequest): DatabaseLogReply {
        LOGGER.debug("getDatabaseLog request received: $request")
        val builder = DatabaseLogReply.newBuilder()
        when (val result = queryService.getDatabaseLog(request.database)) {
            is Either.Left -> builder.databaseNotFoundBuilder.name = result.value.name
            is Either.Right -> builder.logBuilder.addAllBatches(result.value.map { it.toProto() })
        }
        return builder.build()
    }

    override suspend fun getBatch(request: BatchRequest): BatchReply {
        LOGGER.debug("getBatch request received: $request")
        val builder = BatchReply.newBuilder()
        val batchId = BatchId.fromString(request.batchId)
        when (val result = queryService.getBatch(request.database, batchId)
        ) {
            is Either.Left -> builder.batchNotFoundBuilder
                .setDatabase(result.value.database)
                .setBatchId(result.value.batchId.toString())
            is Either.Right -> builder.batch = result.value.toProto()
        }
        return builder.build()
    }

    override suspend fun getDatabaseStreams(request: DatabaseStreamsRequest): DatabaseStreamsReply {
        LOGGER.debug("getDatabaseStreams request received: $request")
        val builder = DatabaseStreamsReply.newBuilder()
        when (val result = queryService.getDatabaseStreams(
            request.database,
            request.revision,
        )) {
            is Either.Left -> builder.databaseNotFoundBuilder.name = request.database
            is Either.Right -> builder.streamsBuilder.addAllStreams(
                result.value.map { it.toProto() }
            )
        }
        return builder.build()
    }

    override suspend fun getStream(request: StreamRequest): StreamReply {
        LOGGER.debug("getStream request received: $request")
        val builder = StreamReply.newBuilder()
        when (val result = queryService.getStream(
            request.database,
            request.databaseRevision,
            request.stream,
        )
        ) {
            is Either.Left -> builder.streamNotFoundBuilder
                .setDatabase(result.value.database)
                .setStream(result.value.stream)
            is Either.Right -> builder.stream = result.value.toProto()
        }
        return builder.build()
    }

    override suspend fun getSubjectStream(request: SubjectStreamRequest): SubjectStreamReply =
        TODO("Not Implemented Yet")

    override suspend fun getEvents(request: EventsRequest): EventsReply {
        LOGGER.debug("getEvents request received: $request")
        val builder = EventsReply.newBuilder()
        val eventIds = request.eventIdsList.map(EventId::fromString)
        when (val result = queryService.getEvent(request.database, eventIds)
        ) {
            is Either.Left ->
                builder.databaseNotFoundBuilder.name = request.database
            is Either.Right ->
                builder.eventsMap.eventsMap.putAll(result.value
                    .map { (id, event) ->
                        Pair(id.toString(), event.toProto())
                    }
                )
        }
        return builder.build()
    }
}