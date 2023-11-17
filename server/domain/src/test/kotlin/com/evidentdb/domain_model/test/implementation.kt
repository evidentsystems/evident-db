package com.evidentdb.domain_model.test

import arrow.core.Either
import com.evidentdb.application.CommandManager
import com.evidentdb.application.CommandService
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import com.evidentdb.event_model.*
import java.time.Instant

class InMemoryDatabaseRepository(
    databases: Iterable<DatabaseSummary> = listOf(),
    streams: Iterable<Map<StreamName, StreamRevision>> = listOf()
): DatabaseRepository {
    private val databases: Map<DatabaseName, ActiveDatabaseCommandModel> =
        databases.fold(mutableMapOf()) { acc, database ->
        acc[database.name] = ActiveDatabaseCommandModel(
            database.name,
            database.created,
            streams.fold(mutableMapOf()) { acc1, stream ->
                acc1[stream.name] = stream.eventIds.size.toLong()
                acc1
            }
        )
        acc
    }

    override fun log(name: DatabaseName): List<LogBatch>? {
        TODO("Not yet implemented")
    }

    override fun database(name: DatabaseName): ActiveDatabaseCommandModel? =
        databases[name]

    override fun database(name: DatabaseName, revision: DatabaseRevision): ActiveDatabaseCommandModel? =
        TODO("Not yet implemented")

    override fun summary(name: DatabaseName): DatabaseSummary? =
        databases[name]?.let { DatabaseSummary(it.name, it.created) }

    override fun catalog(): Set<ActiveDatabaseCommandModel> =
        databases.values.map { ActiveDatabaseCommandModel(it.name, it.created, mapOf()) }.toSet()
}

class InMemoryBatchSummaryRepository(batches: List<LogBatch>): BatchSummaryRepository {
    private val batches: Map<BatchId, LogBatch> =
        batches.fold(mutableMapOf()) { acc, batch ->
            acc[batch.id] = batch
            acc
        }

    override fun batchSummary(database: DatabaseName, id: BatchId): LogBatch? =
        batches[id]
}

class InMemoryCommandHandler(
    databases: List<DatabaseSummary>,
    streams: List<StreamSummary>,
    batches: List<LogBatch>,
): CommandHandler {
    override val databaseRepository = InMemoryDatabaseRepository(databases, streams)
    override val batchSummaryRepository = InMemoryBatchSummaryRepository(batches)
}

class InMemoryCommandManager(
    databases: List<DatabaseSummary>,
    streams: List<StreamSummary>,
    batches: List<LogBatch>,
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
    batches: List<LogBatch>,
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