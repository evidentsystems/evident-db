package com.evidentdb.domain.test

import arrow.core.Either
import com.evidentdb.domain.*
import java.time.Instant
import java.util.*

class InMemoryDatabaseReadModel(
    databases: Iterable<DatabaseSummary> = listOf(),
    streams: Iterable<StreamSummary> = listOf()
): DatabaseReadModel {
    private val databases: Map<DatabaseName, Database> =
        databases.fold(mutableMapOf()) { acc, database ->
        acc[database.name] = Database(
            database.name,
            database.created,
            streams.fold(mutableMapOf()) { acc, stream ->
                acc[stream.name] = stream.eventIds.size.toLong()
                acc
            }
        )
        acc
    }

    override fun database(name: DatabaseName): Database? =
        databases[name]

    override fun database(name: DatabaseName, revision: DatabaseRevision): Database? =
        TODO("Not yet implemented")

    override fun summary(name: DatabaseName): DatabaseSummary? =
        databases[name]?.let { DatabaseSummary(it.name, it.created) }

    override fun catalog(): Set<DatabaseSummary> =
        databases.values.map { DatabaseSummary(it.name, it.created) }.toSet()
}

class InMemoryBatchSummaryReadModel(batches: List<BatchSummary>): BatchSummaryReadModel {
    private val batches: Map<BatchId, List<EventId>> =
        batches.fold(mutableMapOf()) { acc, batch ->
            acc[batch.id] = batch.eventIds
            acc
        }

    override fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary? =
        batches[id]?.let {
            BatchSummary(id, database, it, mapOf())
        }
}

class InMemoryCommandHandler(
    databases: List<DatabaseSummary>,
    streams: List<StreamSummary>,
    batches: List<BatchSummary>,
): CommandHandler {
    override val databaseReadModel = InMemoryDatabaseReadModel(databases, streams)
    override val batchSummaryReadModel = InMemoryBatchSummaryReadModel(batches)
}

class InMemoryCommandManager(
    databases: List<DatabaseSummary>,
    streams: List<StreamSummary>,
    batches: List<BatchSummary>,
): CommandManager {
    private val commandHandler = InMemoryCommandHandler(databases, streams, batches)

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> =
        commandHandler.handleCreateDatabase(command)

    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> =
        commandHandler.handleDeleteDatabase(command)

    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> =
        commandHandler.handleTransactBatch(command)
}

class InMemoryCommandService(
    databases: List<DatabaseSummary>,
    streams: List<StreamSummary>,
    batches: List<BatchSummary>,
): CommandService {
    override val commandManager = InMemoryCommandManager(databases, streams, batches)

    companion object {
        fun empty(): InMemoryCommandService = InMemoryCommandService(listOf(), listOf(), listOf())
    }
}

fun buildTestDatabase(name: DatabaseName) =
    DatabaseSummary(
        name,
        Instant.now(),
    )