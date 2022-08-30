package com.evidentdb.service

import com.evidentdb.domain.*
import com.evidentdb.domain.Database
import com.evidentdb.domain.DatabaseNameAlreadyExistsError
import com.evidentdb.domain.DatabaseNotFoundError
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

class EvidentDbEndpoint(val service: Service)
    : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EvidentDbEndpoint::class.java)
    }

    override suspend fun createDatabase(request: DatabaseCreationInfo): CreateDatabaseReply {
        LOGGER.debug("createDatabase request received: $request")
        val builder = CreateDatabaseReply.newBuilder()
        service.createDatabase(request.name).bimap(
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
                builder.database = Database(it.database).toProto()
            }
        )
        return builder.build()
    }

    // TODO: reply needs database id
    override suspend fun deleteDatabase(request: DatabaseDeletionInfo): DeleteDatabaseReply {
        LOGGER.debug("deleteDatabase request received: $request")
        val builder = DeleteDatabaseReply.newBuilder()
        service.deleteDatabase(request.name)
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
                    builder.databaseDeletionInfo = it.data.toProto()
                }
            )
        return builder.build()
    }

    override suspend fun transactBatch(request: BatchProposal): TransactBatchReply {
        LOGGER.debug("transactBatch request received: $request")
        val builder = TransactBatchReply.newBuilder()
        service.transactBatch(
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
                        is StreamStateConflictsError -> {
                            builder.streamStateConflictError = it.toProto()
                        }
                        is InternalServerError -> {
                            builder.internalServerError = it.toProto()
                        }
                    }
                },
                {
                    builder.batchResultBuilder.id = it.data.id.toString()
                    builder.batchResultBuilder.database = it.database.value
                    builder.batchResultBuilder.addAllEventIds(
                        it.data.events.map { event -> event.id.toString() }
                    )
                }
            )
        return builder.build()
    }
}