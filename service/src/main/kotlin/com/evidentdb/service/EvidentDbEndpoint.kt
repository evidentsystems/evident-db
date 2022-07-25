package com.evidentdb.service

import com.evidentdb.domain.*
import com.evidentdb.domain.DatabaseNameAlreadyExistsError
import com.evidentdb.domain.DatabaseNotFoundError
import com.evidentdb.domain.InvalidDatabaseNameError
import com.evidentdb.domain.InvalidEventsError
import com.evidentdb.domain.NoEventsProvidedError
import com.evidentdb.domain.StreamStateConflictsError
import com.evidentdb.dto.toProto
import com.evidentdb.dto.unvalidatedProposedEventFromProto
import com.evidentdb.dto.v1.proto.*
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo
import com.evidentdb.dto.v1.proto.DatabaseRenameInfo
import com.evidentdb.service.v1.*

class EvidentDbEndpoint(val service: Service)
    : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {
    override suspend fun createDatabase(request: DatabaseCreationInfo): CreateDatabaseReply {
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
                }
            },
            {
                builder.database = it.data.database.toProto()
            }
        )
        return builder.build()
    }

    // TODO: reply needs database id
    override suspend fun renameDatabase(request: DatabaseRenameInfo): RenameDatabaseReply {
        val builder = RenameDatabaseReply.newBuilder()
        service.renameDatabase(request.oldName, request.newName)
            .bimap(
                {
                    when(it) {
                        is DatabaseNameAlreadyExistsError -> {
                            builder.databaseNameAlreadyExistsError = it.toProto()
                        }
                        is DatabaseNotFoundError -> {
                            builder.databaseNotFoundError = it.toProto()
                        }
                        is InvalidDatabaseNameError -> {
                            builder.invalidDatabaseNameError = it.toProto()
                        }
                    }
                },
                {
                    builder.databaseRenameInfo = it.data.toProto()
                }
            )
        return builder.build()
    }

    // TODO: reply needs database id
    override suspend fun deleteDatabase(request: DatabaseDeletionInfo): DeleteDatabaseReply {
        val builder = DeleteDatabaseReply.newBuilder()
        service.deleteDatabase(request.name)
            .bimap(
                {
                    when(it) {
                        is DatabaseNotFoundError -> {
                            builder.databaseNotFoundError = it.toProto()
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
        val builder = TransactBatchReply.newBuilder()
        service.transactBatch(
            request.databaseName,
            request.eventsList.map(::unvalidatedProposedEventFromProto)
        )
            .bimap(
                {
                    when(it) {
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
                    }
                },
                {
                    builder.batchResultBuilder.id = it.data.id.toString()
                    builder.batchResultBuilder.databaseId = it.databaseId.toString()
                    builder.batchResultBuilder.addAllEventIds(
                        it.data.events.map { event -> event.id.toString() }
                    )
                }
            )
        return builder.build()
    }
}