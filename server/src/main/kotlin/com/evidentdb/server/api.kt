package com.evidentdb.server

import com.evidentdb.*
import com.evidentdb.batch.BatchCommandHandler
import com.evidentdb.command.InFlightCoordinator
import com.evidentdb.database.DatabaseCommandHandler
import com.evidentdb.domain.database.CreationProposalOutcome
import com.evidentdb.domain.database.RenamingProposalOutcome
import jakarta.inject.Singleton
import kotlinx.coroutines.flow.Flow

@Singleton
class CommandHandler(
    override val log: KafkaCommandLog,
    override val inFlightCreations: InFlightCoordinator<CreationProposalOutcome>,
    override val inFlightRenames: InFlightCoordinator<RenamingProposalOutcome>
): DatabaseCommandHandler, BatchCommandHandler

@Singleton
@Suppress("unused")
class EvidentDbEndpoint() : EvidentDbGrpcKt.EvidentDbCoroutineImplBase() {

    override fun catalogSubscription(request: FetchCatalogRequest): Flow<Catalog> {
        return super.catalogSubscription(request)
    }

    override suspend fun createDatabase(request: CreateDatabaseRequest): CreateDatabaseReply {
        return super.createDatabase(request)
    }

    override suspend fun renameDatabase(request: RenameDatabaseRequest): RenameDatabaseReply {
        return super.renameDatabase(request)
    }

    override suspend fun deleteDatabase(request: DeleteDatabaseRequest): DeleteDatabaseReply {
        return super.deleteDatabase(request)
    }

}