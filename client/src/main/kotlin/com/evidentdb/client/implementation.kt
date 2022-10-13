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
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor

const val EVENTS_TO_DATABASE_CACHE_SIZE_RATIO = 100
const val LATEST_DATABASE_REVISION_SIGIL = 0L

class EvidentDb(val channel: ManagedChannel) : Client {
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

    private val connections = ConcurrentHashMap<DatabaseName, IConnection>(10)

    override fun createDatabase(name: DatabaseName): Boolean {
        val client = EvidentDbGrpc.newBlockingStub(channel)
        val result = client.createDatabase(
            DatabaseCreationInfo.newBuilder()
                .setName(name)
                .build()
        )
        return when(result.resultCase) {
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

    // TODO: remove connection from cache
    override fun deleteDatabase(name: DatabaseName): Boolean {
        val client = EvidentDbGrpc.newBlockingStub(channel)
        val result = client.deleteDatabase(
            DatabaseDeletionInfo.newBuilder()
                .setName(name)
                .build()
        )
        return when(result.resultCase) {
            DeleteDatabaseReply.ResultCase.DATABASE_DELETION -> true
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

    override fun catalog(): Iterable<DatabaseSummary> {
        val client = EvidentDbGrpc.newBlockingStub(channel)
        val result = client.catalog(CatalogRequest.getDefaultInstance())
        return result.databasesList
            .map { summary ->
                DatabaseSummary(
                    summary.name,
                    summary.created.toInstant(),
                    mapOf() // TODO: include streamRevisions in DatabaseSummary reply
                )
            }
    }

    // TODO: ensure database exists before making connection or
    //  returning cached connection
    override fun connectDatabase(
        name: DatabaseName,
        cacheSize: Long,
    ): IConnection {
        connections[name]?.let {
            return it
        }
        val newConnection = Connection(name, this, cacheSize)
        connections[name] = newConnection
        return newConnection
    }

    fun shutdown(): ManagedChannel? {
        shutdownConnections()
        return channel.shutdown()
    }

    fun shutdownNow(): ManagedChannel? {
        shutdownConnections()
        return channel.shutdownNow()
    }

    private fun shutdownConnections() {
        connections.forEach { (name, connection) ->
            removeConnection(name)
            connection.shutdown()
        }
    }

    fun removeConnection(database: DatabaseName) =
        connections.remove(database)
}

class Connection(
    override val database: DatabaseName,
    private val client: EvidentDb,
    eventCacheSize: Long,
): IConnection {
    private val dbCache: LoadingCache<DatabaseRevision, DatabaseSummary> = Caffeine.newBuilder()
        .maximumSize(eventCacheSize.div(EVENTS_TO_DATABASE_CACHE_SIZE_RATIO))
        .build(DatabaseSummaryLoader(client.channel, database))

    private val streamCache: AsyncLoadingCache<StreamName, List<EventId>> = Caffeine.newBuilder()
        .maximumSize(eventCacheSize)
        .buildAsync(StreamLoader(client.channel, database))

    // TODO: implement event weighting via event data size in bytes
    private val eventCache: AsyncLoadingCache<EventId, CloudEvent> = Caffeine.newBuilder()
        .maximumSize(eventCacheSize)
        .buildAsync(EventLoader(client.channel, database))

    private val grpcClient = EvidentDbGrpc.newBlockingStub(client.channel)

    override fun transact(events: List<EventProposal>): Batch {
        if (events.isEmpty()) throw IllegalArgumentException("No events provided in batch")

        val result = grpcClient.transactBatch(
            BatchProposal.newBuilder()
                .setDatabase(database)
                .addAllEvents(events.map { it.toProto() })
                .build()
        )
        val batch: Batch
        val database: DatabaseSummary
        when(result.resultCase) {
            TransactBatchReply.ResultCase.BATCH_TRANSACTION -> {
                batch = result.batchTransaction.batch.toDomain()
                database = result.batchTransaction.database.toDomain()
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
        dbCache.put(batch.revision, database)
        eventCache.synchronous().putAll(
            batch.events.fold(mutableMapOf()) { acc, event ->
                acc[event.id] = event.event
                acc
            }
        )
        return batch
    }

    override fun db(): IDatabase =
        loadDatabase(grpcClient, database, LATEST_DATABASE_REVISION_SIGIL)!!.let { summary ->
            dbCache.put(summary.revision, summary)
            Database(
                summary.name,
                summary.created,
                summary.streamRevisions,
                streamCache,
                eventCache
            )
        }

    // TODO: validate revision > 0, and ensure
    //  all valid revisions return a valid database (currently throws NPE?)
    override fun dbAsOf(revision: DatabaseRevision): IDatabase =
        dbCache.get(revision).let { summary ->
            Database(
                summary.name,
                summary.created,
                summary.streamRevisions,
                streamCache,
                eventCache
            )
        }

    override fun log(): Iterable<Batch> {
        val log = mutableListOf<Batch>()
        for (batch in grpcClient.databaseLog(
            DatabaseLogRequest
                .newBuilder()
                .setDatabase(database)
                .build())) {
            val batchSummary = batch.batch
            val batchEvents = eventCache.getAll(
                batchSummary.eventsList.map { EventId.fromString(it.id) }
            ).get()
            log.add(
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
            )
        }
        return log
    }

    override fun shutdown() {
        eventCache.synchronous().invalidateAll()
        client.removeConnection(database)
    }

    fun eventCacheStats() = eventCache.synchronous().stats()
}

data class Database(
    val name: DatabaseName,
    val created: Instant,
    override val streamRevisions: Map<StreamName, StreamRevision>,
    val streamCache: AsyncLoadingCache<StreamName, List<EventId>>,
    val eventCache: AsyncLoadingCache<EventId, CloudEvent>,
) : IDatabase {
    private val streams = ConcurrentHashMap<StreamName, List<EventId>>(streamRevisions.size)

    override val revision: DatabaseRevision
        get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        }

    override fun stream(streamName: StreamName): Iterable<CloudEvent>? {
        val eventCount = streamRevisions[streamName]
            ?: throw java.lang.IllegalArgumentException("No stream $streamName found " +
                    "for database $name")
        return (streams[streamName] ?: streamCache[streamName].get())?.let { eventIds ->
            val refreshedEventIds = (if (eventIds.size < eventCount)
                streamCache.synchronous().refresh(streamName).get()
            else
                eventIds).take(eventCount.toInt())
            streams[streamName] = refreshedEventIds
            val events = eventCache.getAll(refreshedEventIds).get()
            refreshedEventIds.map { events[it]!! }
        }!!
    }

    override fun subjectStream(streamName: StreamName, subjectName: StreamSubject): Iterable<CloudEvent> {
        TODO("Not yet implemented")
    }

    override fun event(eventId: EventId): CloudEvent? =
        eventCache[eventId].get()
}

fun loadDatabase(
    grpcClient: EvidentDbGrpc.EvidentDbBlockingStub,
    database: DatabaseName,
    revision: DatabaseRevision,
): DatabaseSummary? {
    val builder = DatabaseRequest.newBuilder()
        .setName(database)
    if (revision > LATEST_DATABASE_REVISION_SIGIL)
        builder.revision = revision
    val result = grpcClient.database(builder.build())
    return when(result.resultCase) {
        DatabaseReply.ResultCase.DATABASE -> DatabaseSummary(
            database,
            result.database.created.toInstant(),
            result.database.streamRevisionsMap,
        )
        DatabaseReply.ResultCase.NOT_FOUND -> null
        else -> throw IllegalStateException("Result not set")
    }
}

class DatabaseSummaryLoader(
    channel: Channel,
    private val database: DatabaseName
) : CacheLoader<DatabaseRevision, DatabaseSummary> {
    private val grpcClient = EvidentDbGrpc.newBlockingStub(channel)

    override fun load(revision: DatabaseRevision): DatabaseSummary? =
        loadDatabase(grpcClient, database, revision)
}

class StreamLoader(
    channel: Channel,
    private val database: DatabaseName,
): AsyncCacheLoader<StreamName, List<EventId>> {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)

    override fun asyncLoad(
        key: StreamName?,
        executor: Executor?
    ): CompletableFuture<out List<EventId>> {
        requireNotNull(key)
        requireNotNull(executor)
        return runBlocking(executor.asCoroutineDispatcher()) {
            future {
                fetchStreamEventIds(key)
            }
        }
    }

    private suspend fun fetchStreamEventIds(stream: StreamName): List<EventId> {
        val result = grpcClient.stream(
            StreamRequest.newBuilder()
                .setDatabase(database)
                .setStream(stream)
                .build()
        )
        return result.map { reply ->
            when(reply.resultCase) {
                StreamEventIdReply.ResultCase.EVENT_ID ->
                    EventId.fromString(reply.eventId)
                StreamEventIdReply.ResultCase.STREAM_NOT_FOUND ->
                    throw IllegalStateException("Stream \"${reply.streamNotFound.stream}\" " +
                            "for database \"${reply.streamNotFound.database}\"")
                else -> throw IllegalStateException("Result not set")
            }
        }.toList()
    }
}

class EventLoader(
    channel: Channel,
    private val database: DatabaseName,
): AsyncCacheLoader<EventId, CloudEvent> {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)

    override fun asyncLoad(
        key: EventId?,
        executor: Executor?
    ): CompletableFuture<CloudEvent?> {
        requireNotNull(key)
        requireNotNull(executor)

        return runBlocking(executor.asCoroutineDispatcher()) {
            future {
                fetchEvents(listOf(key))[key]
            }
        }
    }

    override fun asyncLoadAll(
        keys: MutableSet<out EventId>?,
        executor: Executor?
    ): CompletableFuture<out Map<out EventId, CloudEvent>> {
        requireNotNull(keys)
        requireNotNull(executor)

        return runBlocking(executor.asCoroutineDispatcher()) {
            future {
                fetchEvents(keys.toList())
            }
        }
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
        result.collect {reply ->
            when(reply.resultCase) {
                EventReply.ResultCase.EVENT_MAP_ENTRY -> {
                    val entry = reply.eventMapEntry
                    val id = EventId.fromString(entry.eventId)
                    eventMap[id] = entry.event.toDomain()
                }
                EventReply.ResultCase.EVENT_NOT_FOUND ->
                    throw IllegalStateException("Event ${reply.eventNotFound.eventId} " +
                            "not found for Database ${reply.eventNotFound.database}")
                else -> throw IllegalStateException("Result not set")
            }
        }
        return eventMap
    }
}
