package com.evidentdb.domain.test

import arrow.core.Either
import com.evidentdb.domain.*

class InMemoryDatabaseReadModel(
    private val databases: Map<DatabaseId, Database> = mapOf(),
): DatabaseReadModel {
    private val databaseNames = databases.values.fold(
        mutableMapOf<DatabaseName, DatabaseId>()
    ) { acc, database ->
        acc[database.name] = database.id
        acc
    }

    override suspend fun database(databaseId: DatabaseId): Database? =
        databases[databaseId]

    override suspend fun database(name: DatabaseName): Database? =
        databases[databaseNames[name]]

    override suspend fun catalog(): Set<Database> =
        databases.values.toSet()
}

class InMemoryStreamReadModel(
    private val streams: Map<StreamKey, List<EventId>> = mapOf()
): StreamReadModel {

    override suspend fun streamState(databaseId: DatabaseId, name: StreamName): StreamState {
        val eventIds = streams[buildStreamKey(databaseId, name)] ?: return StreamState.NoStream
        return StreamState.AtRevision(eventIds.size.toLong())
    }

    override suspend fun stream(databaseId: DatabaseId, name: StreamName): Stream? {
        val eventIds = streams[buildStreamKey(databaseId, name)] ?: return null
        return Stream.create(databaseId, name, eventIds.size.toLong())
    }

    override suspend fun streamEventIds(streamKey: StreamKey): List<EventId>? =
        streams[streamKey]

    override suspend fun databaseStreams(databaseId: DatabaseId): Set<Stream> =
        streams.map { (streamKey, eventIds) ->
            val (dbId, name) = parseStreamKey(streamKey)
            if (dbId == databaseId) {
                Stream.create(dbId, name, eventIds.size.toLong())
            } else {
                null
            }
        }.filterNotNull().toSet()
}

class InMemoryCommandHandler(
    databases: Map<DatabaseId, Database>,
    streams: Map<StreamKey, List<EventId>>,
): CommandHandler {
    override val databaseReadModel = InMemoryDatabaseReadModel(databases)
    override val streamReadModel = InMemoryStreamReadModel(streams)
}

class InMemoryCommandBroker(
    databases: Map<DatabaseId, Database>,
    streams: Map<StreamKey, List<EventId>>,
): CommandBroker {
    private val commandHandler = InMemoryCommandHandler(databases, streams)

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> =
        commandHandler.handleCreateDatabase(command)

    override suspend fun renameDatabase(command: RenameDatabase): Either<DatabaseRenameError, DatabaseRenamed> =
        commandHandler.handleRenameDatabase(command)

    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> =
        commandHandler.handleDeleteDatabase(command)

    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> =
        commandHandler.handleTransactBatch(command)
}

class InMemoryService(
    databases: Map<DatabaseId, Database>,
    streams: Map<StreamKey, List<EventId>>,
): Service {
    override val databaseReadModel = InMemoryDatabaseReadModel(databases)
    override val commandBroker = InMemoryCommandBroker(databases, streams)

    companion object {
        fun empty(): InMemoryService = InMemoryService(mapOf(), mapOf())
    }
}