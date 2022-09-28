package com.evidentdb.service

import com.evidentdb.domain.*
import com.evidentdb.domain.DatabaseNameAlreadyExistsError
import com.evidentdb.domain.DatabaseNotFoundError
import com.evidentdb.domain.DuplicateBatchError
import com.evidentdb.domain.InternalServerError
import com.evidentdb.domain.InvalidDatabaseNameError
import com.evidentdb.domain.InvalidEventsError
import com.evidentdb.domain.NoEventsProvidedError
import com.evidentdb.domain.StreamStateConflictsError
import com.evidentdb.dto.toProto
import com.evidentdb.dto.unvalidatedProposedEventFromProto
import com.evidentdb.dto.v1.proto.*
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo
import com.evidentdb.service.v1.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EvidentDbEndpoint(private val commandService: CommandService)
    : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {
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
}