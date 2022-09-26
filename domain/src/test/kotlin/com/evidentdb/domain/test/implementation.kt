package com.evidentdb.domain.test

import arrow.core.Either
import com.evidentdb.domain.*

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

class InMemoryStreamReadModel(
    streams: Iterable<Stream>
): StreamReadModel {
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

    override fun stream(databaseName: DatabaseName, name: StreamName): Stream? {
        val eventIds = streams[buildStreamKey(databaseName, name)] ?: return null
        return Stream.create(databaseName, name, eventIds.size.toLong())
    }

    override fun streamEventIds(streamKey: StreamKey): List<EventId>? =
        streams[streamKey]

    override fun databaseStreams(databaseName: DatabaseName): Set<Stream> =
        streams.map { (streamKey, eventIds) ->
            val (dbId, name) = parseStreamKey(streamKey)
            if (dbId == databaseName) {
                Stream.create(dbId, name, eventIds.size.toLong())
            } else {
                null
            }
        }.filterNotNull().toSet()
}

class InMemoryBatchReadModel(batches: List<Batch>): BatchReadModel {
    private val batches: Map<BatchId, Batch> =
        batches.fold(mutableMapOf()) { acc, batch ->
            acc[batch.id] = batch
            acc
        }

    override fun batch(id: BatchId): Batch? =
        batches[id]

    override fun batchSummary(id: BatchId): BatchSummary? =
        batch(id)?.let {
            BatchSummary(it.database, it.events.map { event -> event.id })
        }

}

class InMemoryCommandHandler(
    databases: List<Database>,
    streams: List<Stream>,
    batches: List<Batch>,
): CommandHandler {
    override val databaseReadModel = InMemoryDatabaseReadModel(databases)
    override val streamReadModel = InMemoryStreamReadModel(streams)
    override val batchReadModel = InMemoryBatchReadModel(batches)
}

class InMemoryCommandManager(
    databases: List<Database>,
    streams: List<Stream>,
    batches: List<Batch>,
): CommandManager {
    private val commandHandler = InMemoryCommandHandler(databases, streams, batches)

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> =
        commandHandler.handleCreateDatabase(command)

    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> =
        commandHandler.handleDeleteDatabase(command)

    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> =
        commandHandler.handleTransactBatch(command)
}

class InMemoryService(
    databases: List<Database>,
    streams: List<Stream>,
    batches: List<Batch>,
): Service {
    override val databaseReadModel = InMemoryDatabaseReadModel(databases)
    override val commandManager = InMemoryCommandManager(databases, streams, batches)

    companion object {
        fun empty(): InMemoryService = InMemoryService(listOf(), listOf(), listOf())
    }
}