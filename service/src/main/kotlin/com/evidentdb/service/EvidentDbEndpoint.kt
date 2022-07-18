package com.evidentdb.service

import com.evidentdb.domain.Service
import com.evidentdb.dto.v1.proto.BatchProposal
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo
import com.evidentdb.dto.v1.proto.DatabaseRenameInfo
import com.evidentdb.service.v1.*

class EvidentDbEndpoint(val service: Service)
    : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {
    override suspend fun createDatabase(request: DatabaseCreationInfo): CreateDatabaseReply {
        TODO("Implement in terms of service")
    }

    override suspend fun renameDatabase(request: DatabaseRenameInfo): RenameDatabaseReply {
        TODO("Implement in terms of service")
    }

    override suspend fun deleteDatabase(request: DatabaseDeletionInfo): DeleteDatabaseReply {
        TODO("Implement in terms of service")
    }

    override suspend fun transactBatch(request: BatchProposal): TransactBatchReply {
        TODO("Implement in terms of service")
    }
}