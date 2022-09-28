package com.evidentdb.domain.test

import arrow.core.Either
import com.evidentdb.domain.*
import java.time.Instant
import java.util.*

class InMemoryDatabaseReadModel(
    databases: Iterable<Database> = listOf()
): DatabaseReadModel {
    private val databases: Map<DatabaseName, Database> =
        databases.fold(mutableMapOf()) { acc, database ->
        acc[database.name] = database
        acc
    }

    override fun database(name: DatabaseName): Database? =
        databases[name]

    override fun catalog(): Set<Database> =
        databases.values.toSet()
}

class InMemoryStreamSummaryReadModel(
    streams: Iterable<StreamSummary>
): StreamSummaryReadModel {
    private val streams: Map<StreamKey, List<EventId>> =
        streams.fold(mutableMapOf()) { acc, stream ->
            acc[buildStreamKey(stream.database, stream.name)] = listOf()
            acc
        }

    override fun streamState(databaseName: DatabaseName, name: StreamName): StreamState {
        val eventIds = streams[buildStreamKey(databaseName, name)]
            ?: return StreamState.NoStream
        return StreamState.AtRevision(eventIds.size.toLong())
    }

    override fun streamSummary(streamKey: StreamKey): StreamSummary? {
        val eventIds = streams[streamKey] ?: return null
        return StreamSummary.create(streamKey, eventIds)
    }

    override fun databaseStreams(databaseName: DatabaseName): Set<StreamSummary> =
        streams.map { (streamKey, eventIds) ->
            val (dbId, name) = parseStreamKey(streamKey)
            if (dbId == databaseName) {
                StreamSummary.create(dbId, name, eventIds = eventIds)
            } else {
                null
            }
        }.filterNotNull().toSet()
}

class InMemoryBatchSummaryReadModel(batches: List<BatchSummary>): BatchSummaryReadModel {
    private val batches: Map<BatchKey, List<EventId>> =
        batches.fold(mutableMapOf()) { acc, batch ->
            acc[buildBatchKey(batch.database, batch.id)] = batch.eventIds
            acc
        }

    override fun batchSummary(database: DatabaseName, id: BatchId): BatchSummary? =
        batches[buildBatchKey(database, id)]?.let {
            BatchSummary(id, database, it)
        }
}

class InMemoryCommandHandler(
    databases: List<Database>,
    streams: List<StreamSummary>,
    batches: List<BatchSummary>,
): CommandHandler {
    override val databaseReadModel = InMemoryDatabaseReadModel(databases)
    override val streamSummaryReadModel = InMemoryStreamSummaryReadModel(streams)
    override val batchSummaryReadModel = InMemoryBatchSummaryReadModel(batches)
}

class InMemoryCommandManager(
    databases: List<Database>,
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
    databases: List<Database>,
    streams: List<StreamSummary>,
    batches: List<BatchSummary>,
): CommandService {
    override val commandManager = InMemoryCommandManager(databases, streams, batches)

    companion object {
        fun empty(): InMemoryCommandService = InMemoryCommandService(listOf(), listOf(), listOf())
    }
}

fun buildTestDatabase(name: DatabaseName) =
    Database(
        name,
        Instant.now(),
        UUID.randomUUID(),
    )