package com.evidentdb.client

import arrow.core.foldLeft
import com.evidentdb.dto.v1.proto.BatchProposal
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo
import com.evidentdb.service.v1.*
import com.github.benmanes.caffeine.cache.*
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.CloudEventExtension
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.protobuf.toDomain
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.net.URI
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference

const val EVENTS_TO_DATABASE_CACHE_SIZE_RATIO = 100
const val LATEST_DATABASE_REVISION_SIGIL = 0L

class EvidentDB(val channelBuilder: ManagedChannelBuilder<*>) : Client {
    private val clientScope = CoroutineScope(Dispatchers.Default)
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channelBuilder.build())

    private val connections = ConcurrentHashMap<DatabaseName, IConnection>(10)

    override fun createDatabase(name: DatabaseName): Boolean = runBlocking(clientScope.coroutineContext) {
        val result = grpcClient.createDatabase(
            DatabaseCreationInfo.newBuilder()
                .setName(name)
                .build()
        )
        when (result.resultCase) {
            CreateDatabaseReply.ResultCase.DATABASE_CREATION ->
                true
            CreateDatabaseReply.ResultCase.INVALID_DATABASE_NAME_ERROR ->
                throw IllegalArgumentException("Invalid database name: $name")
            CreateDatabaseReply.ResultCase.DATABASE_NAME_ALREADY_EXISTS_ERROR ->
                throw IllegalArgumentException("Database name already exists: $name")
            CreateDatabaseReply.ResultCase.INTERNAL_SERVER_ERROR ->
                throw IllegalStateException("Internal server error: ${result.internalServerError.message}")
            else ->
                throw IllegalStateException("Result not set")
        }
    }

    override fun deleteDatabase(name: DatabaseName): Boolean = runBlocking(clientScope.coroutineContext) {
        val result = grpcClient.deleteDatabase(
            DatabaseDeletionInfo.newBuilder()
                .setName(name)
                .build()
        )
        when (result.resultCase) {
            DeleteDatabaseReply.ResultCase.DATABASE_DELETION -> {
                removeConnection(name)?.shutdown()
                true
            }
            DeleteDatabaseReply.ResultCase.INVALID_DATABASE_NAME_ERROR ->
                throw IllegalArgumentException("Invalid database name: $name")
            DeleteDatabaseReply.ResultCase.DATABASE_NOT_FOUND_ERROR ->
                throw IllegalStateException("Database not found: $name")
            DeleteDatabaseReply.ResultCase.INTERNAL_SERVER_ERROR ->
                throw IllegalStateException("Internal server error: ${result.internalServerError.message}")
            else ->
                throw IllegalStateException("Result not set")
        }
    }

    override fun catalog(): Iterable<DatabaseSummary> = runBlocking(clientScope.coroutineContext) {
        val result = grpcClient.catalog(CatalogRequest.getDefaultInstance())
        result.databasesList.map { summary ->
            DatabaseSummary(
                summary.name,
                summary.created.toInstant(),
                mapOf() // TODO: include streamRevisions in DatabaseSummary reply
            )
        }
    }

    override fun connectDatabase(
        name: DatabaseName,
        cacheSize: Long,
    ): IConnection {
        connections[name]?.let {
            return it
        }
        val newConnection = Connection(name, this, cacheSize)
        newConnection.start()
        connections[name] = newConnection
        return newConnection
    }

    fun shutdown() {
        clientScope.cancel()
        connections.forEach { (name, connection) ->
            removeConnection(name)
            connection.shutdown()
        }
    }

    fun shutdownNow() {
        clientScope.cancel()
        connections.forEach { (name, connection) ->
            removeConnection(name)
            connection.shutdownNow()
        }
    }

    fun removeConnection(database: DatabaseName) =
        connections.remove(database)

    companion object {
        @JvmStatic
        fun cloudevent(
            eventType: String,
            data: CloudEventData? = null,
            dataContentType: String? = null,
            dataSchema: URI? = null,
            subject: String? = null,
            extensions: List<CloudEventExtension>,
        ): CloudEvent {
            val builder = CloudEventBuilder.v1()
                .withId("1")
                .withSource(URI("edb:source"))
                .withType(eventType)
                .withData(data)
                .withDataContentType(dataContentType)
                .withDataSchema(dataSchema)
                .withSubject(subject)
            for (extension in extensions)
                builder.withExtension(extension)
            return builder.build()
        }
    }
}

class Connection(
    override val database: DatabaseName,
    private val client: EvidentDB,
    eventCacheSize: Long,
) : IConnection {
    private val connectionScope = CoroutineScope(Dispatchers.Default)
    private val latestRevision: AtomicReference<DatabaseSummary> = AtomicReference()

    private val dbCacheChannel = client.channelBuilder.build()
    private val dbCache: AsyncCache<DatabaseRevision, DatabaseSummary> =
        Caffeine.newBuilder()
            .maximumSize(eventCacheSize.div(EVENTS_TO_DATABASE_CACHE_SIZE_RATIO))
            .buildAsync()

    private val streamCacheChannel = client.channelBuilder.build()
    private val streamCache: AsyncLoadingCache<StreamName, List<EventId>> =
        Caffeine.newBuilder()
            .maximumSize(eventCacheSize)
            .buildAsync(StreamLoader(streamCacheChannel, database))

    private val eventCacheChannel = client.channelBuilder.build()

    // TODO: implement event weighting via event data size in bytes
    private val eventCache: AsyncLoadingCache<EventId, CloudEvent> =
        Caffeine.newBuilder()
            .maximumSize(eventCacheSize)
            .buildAsync(EventLoader(eventCacheChannel, database))

    private val grpcClientChannel = client.channelBuilder.build()
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(grpcClientChannel)

    fun start() {
        connectionScope.launch {
            grpcClient.connect(
                DatabaseRequest.newBuilder()
                    .setName(database)
                    .build()
            ).collect { reply ->
                when(reply.resultCase) {
                    DatabaseReply.ResultCase.DATABASE -> {
                        val summary = reply.database.toDomain()
                        setLatestRevision(summary)
                        dbCache.get(summary.revision) { _ -> summary }
                    }
                    DatabaseReply.ResultCase.NOT_FOUND -> {
                        shutdownNow()
                        throw IllegalStateException("Database not found: $database")
                    }
                    else -> throw IllegalStateException("Result not set")
                }
            }
        }
    }

    private fun setLatestRevision(database: DatabaseSummary) {
        latestRevision.getAndUpdate { current ->
            if (current == null || (database.revision > current.revision))
                database
            else
                current
        }
    }

    override fun transact(events: List<EventProposal>): CompletableFuture<Batch> =
        connectionScope.future {
            if (events.isEmpty()) throw IllegalArgumentException("No events provided in batch")

            val result = grpcClient.transactBatch(
                BatchProposal.newBuilder()
                    .setDatabase(database)
                    .addAllEvents(events.map { it.toProto() })
                    .build()
            )
            when (result.resultCase) {
                TransactBatchReply.ResultCase.BATCH_TRANSACTION -> {
                    val database = result.batchTransaction.database.toDomain()
                    setLatestRevision(database)
                    dbCache.get(database.revision) { _ -> database }

                    val batch = result.batchTransaction.batch.toDomain()
                    eventCache.synchronous().putAll(
                        batch.events.fold(mutableMapOf()) { acc, event ->
                            acc[event.id] = event.event
                            acc
                        }
                    )

                    batch
                }
                TransactBatchReply.ResultCase.INVALID_DATABASE_NAME_ERROR ->
                    throw IllegalArgumentException("Invalid database name: $database")
                TransactBatchReply.ResultCase.DATABASE_NOT_FOUND_ERROR ->
                    throw IllegalStateException("Database not found: $database")
                TransactBatchReply.ResultCase.NO_EVENTS_PROVIDED_ERROR ->
                    throw IllegalArgumentException("No events provided in batch")
                TransactBatchReply.ResultCase.INVALID_EVENTS_ERROR ->
                    // TODO: format this error better
                    throw IllegalArgumentException("Invalid events: ${result.invalidEventsError.invalidEventsList}")
                TransactBatchReply.ResultCase.STREAM_STATE_CONFLICT_ERROR ->
                    // TODO: format this error better
                    throw IllegalStateException("Stream state conflicts: ${result.streamStateConflictError.conflictsList}")
                TransactBatchReply.ResultCase.INTERNAL_SERVER_ERROR ->
                    throw IllegalStateException("Internal server error: ${result.internalServerError.message}")
                TransactBatchReply.ResultCase.DUPLICATE_BATCH_ERROR ->
                    throw IllegalStateException("Internal server error: duplicate batch")
                else -> throw IllegalStateException("Result not set")
            }
        }

    override fun db(): IDatabase =
        latestRevision.get().let { summary ->
            Database(
                summary.name,
                summary.created,
                summary.streamRevisions,
                this,
            )
        }

    // TODO: validate revision > 0, and ensure
    //  all valid revisions return a valid database
    //  event fuzzy (currently throws NPE if not an exact match?)
    override fun db(revision: DatabaseRevision): CompletableFuture<IDatabase> =
        dbCache.get(revision) { _, executor ->
            connectionScope.future(executor.asCoroutineDispatcher()) {
                val summary = loadDatabase(grpcClient, database, revision)
                setLatestRevision(summary)
                summary
            }
        }.thenApply { summary ->
            Database(
                summary.name,
                summary.created,
                summary.streamRevisions,
                this,
            )
        }

    override fun sync(): CompletableFuture<IDatabase> = connectionScope.future {
        loadDatabase(grpcClient, database, LATEST_DATABASE_REVISION_SIGIL).let { summary ->
            setLatestRevision(summary)
            dbCache.get(summary.revision) { _ -> summary }
            Database(
                summary.name,
                summary.created,
                summary.streamRevisions,
                this@Connection,
            )
        }
    }

    override fun log(): Iterable<Batch> = runBlocking(connectionScope.coroutineContext) {
        grpcClient.databaseLog(
            DatabaseLogRequest
                .newBuilder()
                .setDatabase(database)
                .build()
        ).map { batch ->
            val batchSummary = batch.batch
            val batchEvents = eventCache.getAll(
                batchSummary.eventsList.map { EventId.fromString(it.id) }
            ).await()
            Batch(
                BatchId.fromString(batchSummary.id),
                database,
                batchSummary.eventsList.map {
                    Event(
                        batchEvents[EventId.fromString(it.id)]!!,
                        it.stream
                    )
                },
                batchSummary.streamRevisionsMap
            )
        }.toList()
    }

    fun trimAndHydrateStream(
        streamName: StreamName,
        eventCount: Int,
        databaseCachedStreams: ConcurrentHashMap<StreamName, List<EventId>>,
    ): CompletableFuture<List<CloudEvent>> = connectionScope.future {
        val eventIds = databaseCachedStreams[streamName] ?: streamCache[streamName].await()
        val refreshedEventIds = if (eventIds.size < eventCount)
            streamCache.synchronous().refresh(streamName).await()
        else
            eventIds
        val trimmedEventIds = refreshedEventIds.take(eventCount)
        databaseCachedStreams[streamName] = trimmedEventIds
        val events = eventCache.getAll(trimmedEventIds).await()
        trimmedEventIds.map { events[it]!! }
    }

    fun getCachedEvent(eventId: EventId): CompletableFuture<CloudEvent?> =
        eventCache[eventId]

    override fun shutdown() {
        connectionScope.cancel()
        doToAllChannels { it.shutdown() }
        invalidateCaches()
        client.removeConnection(database)
    }

    override fun shutdownNow() {
        connectionScope.cancel()
        doToAllChannels { it.shutdownNow() }
        invalidateCaches()
        client.removeConnection(database)
    }

    private fun doToAllChannels(block: (ManagedChannel) -> Unit) =
        listOf(
            dbCacheChannel,
            streamCacheChannel,
            eventCacheChannel,
            grpcClientChannel
        ).forEach(block)

    private fun invalidateCaches() {
        dbCache.synchronous().invalidateAll()
        streamCache.synchronous().invalidateAll()
        eventCache.synchronous().invalidateAll()
    }

    fun databaseCacheStats() = dbCache.synchronous().stats()
    fun streamCacheStats() = streamCache.synchronous().stats()
    fun eventCacheStats() = eventCache.synchronous().stats()
}

data class Database(
    override val name: DatabaseName,
    override val created: Instant,
    override val streamRevisions: Map<StreamName, StreamRevision>,
    val connection: Connection,
) : IDatabase {
    private val streams = ConcurrentHashMap<StreamName, List<EventId>>(streamRevisions.size)

    override val revision: DatabaseRevision
        get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        }

    override fun stream(streamName: StreamName): CompletableFuture<List<CloudEvent>> {
        val eventCount = streamRevisions[streamName]
            ?: throw java.lang.IllegalArgumentException(
                "No stream $streamName found " +
                        "for database $name"
            )
        return connection.trimAndHydrateStream(
            streamName,
            eventCount.toInt(),
            streams
        )
    }

    override fun subjectStream(
        streamName: StreamName,
        subjectName: StreamSubject
    ): CompletableFuture<List<CloudEvent>?> {
        TODO("Not yet implemented")
    }

    override fun event(eventId: EventId): CompletableFuture<CloudEvent?> =
        connection.getCachedEvent(eventId)
}

suspend fun loadDatabase(
    grpcClient: EvidentDbGrpcKt.EvidentDbCoroutineStub,
    database: DatabaseName,
    revision: DatabaseRevision,
): DatabaseSummary {
    val builder = DatabaseRequest.newBuilder()
        .setName(database)
    if (revision > LATEST_DATABASE_REVISION_SIGIL)
        builder.revision = revision
    val result = grpcClient.database(builder.build())
    return when (result.resultCase) {
        DatabaseReply.ResultCase.DATABASE -> DatabaseSummary(
            database,
            result.database.created.toInstant(),
            result.database.streamRevisionsMap,
        )

        DatabaseReply.ResultCase.NOT_FOUND ->
            throw IllegalStateException("Database not found: $database")
        else -> throw IllegalStateException("Result not set")
    }
}

class StreamLoader(
    channel: Channel,
    private val database: DatabaseName,
) : AsyncCacheLoader<StreamName, List<EventId>> {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)

    override fun asyncLoad(
        key: StreamName?,
        executor: Executor?
    ): CompletableFuture<out List<EventId>> = runBlocking(executor!!.asCoroutineDispatcher()) {
        future { fetchStreamEventIds(key!!) }
    }

    private suspend fun fetchStreamEventIds(stream: StreamName): List<EventId> {
        val result = grpcClient.stream(
            StreamRequest.newBuilder()
                .setDatabase(database)
                .setStream(stream)
                .build()
        )
        return result.map { reply ->
            when (reply.resultCase) {
                StreamEventIdReply.ResultCase.EVENT_ID ->
                    EventId.fromString(reply.eventId)

                StreamEventIdReply.ResultCase.STREAM_NOT_FOUND ->
                    throw IllegalStateException(
                        "Stream \"${reply.streamNotFound.stream}\" " +
                                "for database \"${reply.streamNotFound.database}\""
                    )

                else -> throw IllegalStateException("Result not set")
            }
        }.toList()
    }
}

class EventLoader(
    channel: Channel,
    private val database: DatabaseName,
) : AsyncCacheLoader<EventId, CloudEvent> {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)

    override fun asyncLoad(
        key: EventId?,
        executor: Executor?
    ): CompletableFuture<CloudEvent?> = runBlocking(executor!!.asCoroutineDispatcher()) {
        future { fetchEvents(listOf(key!!))[key] }
    }

    override fun asyncLoadAll(
        keys: MutableSet<out EventId>?,
        executor: Executor?
    ): CompletableFuture<out Map<out EventId, CloudEvent>> =
        runBlocking(executor!!.asCoroutineDispatcher()) {
            future { fetchEvents(keys!!.toList()) }
        }

    private suspend fun fetchEvents(
        eventIds: List<EventId>
    ): Map<EventId, CloudEvent> {
        val result = grpcClient.events(
            eventIds.map {
                EventRequest.newBuilder()
                    .setDatabase(database)
                    .setEventId(it.toString())
                    .build()
            }.asFlow()
        )
        val eventMap = mutableMapOf<EventId, CloudEvent>()
        result.collect { reply ->
            when (reply.resultCase) {
                EventReply.ResultCase.EVENT_MAP_ENTRY -> {
                    val entry = reply.eventMapEntry
                    val id = EventId.fromString(entry.eventId)
                    eventMap[id] = entry.event.toDomain()
                }

                EventReply.ResultCase.EVENT_NOT_FOUND ->
                    throw IllegalStateException(
                        "Event ${reply.eventNotFound.eventId} " +
                                "not found for Database ${reply.eventNotFound.database}"
                    )

                else -> throw IllegalStateException("Result not set")
            }
        }
        return eventMap
    }
}
