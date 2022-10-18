package com.evidentdb.client.kotlin

import arrow.core.foldLeft
import com.evidentdb.client.common.*
import com.evidentdb.client.dto.protobuf.toDomain
import com.evidentdb.client.dto.protobuf.toInstant
import com.evidentdb.client.dto.protobuf.toProto
import com.evidentdb.dto.v1.proto.BatchProposal
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo
import com.evidentdb.service.v1.*
import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.stats.CacheStats
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.CloudEventExtension
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.protobuf.toDomain
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.net.URI
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.ThreadSafe

internal const val DEFAULT_CACHE_SIZE: Long = 10_000 // TODO: Make configurable
internal const val EVENTS_TO_DATABASE_CACHE_SIZE_RATIO = 100
internal const val AVERAGE_EVENT_WEIGHT = 1000
internal const val EMPTY_EVENT_WEIGHT = 10
internal const val LATEST_DATABASE_REVISION_SIGIL = 0L
internal const val GRPC_CONNECTION_SHUTDOWN_TIMEOUT = 30L

/**
 * This is the top-level entry point for the EvidentDB Kotlin client. Use
 * instances of this client to create and delete databases, show
 * the catalog of all available databases, and get connections to
 * a specific database.
 *
 * Clients do not follow an acquire-use-release pattern, and are thread-safe and long-lived.
 * When a program is finished communicating with an EvidentDB server (e.g.
 * at program termination), the client can be cleanly [shutdown] (shutting down
 * and removing all cached [Connection]s after awaiting in-flight requests to complete),
 * or urgently [shutdownNow] (shutting down and removing all cached [Connection]s
 * but not awaiting in-flight requests to complete). Subsequent API method calls will
 * throw [ClientClosedException].
 *
 * @param channelBuilder The gRPC [io.grpc.ManagedChannelBuilder]
 * used to connect to the EvidentDB server.
 * @property cacheSize Configurable size for new connections created by this client
 * @constructor Main entry point for creating EvidentDB clients
 */
@ThreadSafe
class EvidentDB(private val channelBuilder: ManagedChannelBuilder<*>) : Client {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channelBuilder.build())
    private val connections = ConcurrentHashMap<DatabaseName, Connection>(10)
    private val cacheSizeReference = AtomicLong(DEFAULT_CACHE_SIZE)
    private val active = AtomicBoolean(true)

    private val isActive: Boolean
        get() = active.get()

    var cacheSize: Long
        get() = cacheSizeReference.get()
        set(value) {
            cacheSizeReference.set(value)
        }

    override suspend fun createDatabase(name: DatabaseName): Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            val result = grpcClient.createDatabase(
                DatabaseCreationInfo.newBuilder()
                    .setName(name)
                    .build()
            )
            when (result.resultCase) {
                CreateDatabaseReply.ResultCase.DATABASE_CREATION ->
                    true

                CreateDatabaseReply.ResultCase.INVALID_DATABASE_NAME_ERROR ->
                    throw InvalidDatabaseNameError(name)

                CreateDatabaseReply.ResultCase.DATABASE_NAME_ALREADY_EXISTS_ERROR ->
                    throw DatabaseNameAlreadyExistsError(name)

                CreateDatabaseReply.ResultCase.INTERNAL_SERVER_ERROR ->
                    throw InternalServerError(result.internalServerError.message)

                else ->
                    throw SerializationError("Result not set")
            }
        }

    override suspend fun deleteDatabase(name: DatabaseName): Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
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
                    throw InvalidDatabaseNameError(name)

                DeleteDatabaseReply.ResultCase.DATABASE_NOT_FOUND_ERROR ->
                    throw DatabaseNotFoundError(name)

                DeleteDatabaseReply.ResultCase.INTERNAL_SERVER_ERROR ->
                    throw InternalServerError(result.internalServerError.message)

                else ->
                    throw SerializationError("Result not set")
            }
        }

    override fun catalog(): Flow<DatabaseSummary> =
        if (!isActive)
            throw ClientClosedException(this)
        else
            grpcClient
                .catalog(CatalogRequest.getDefaultInstance())
                .map { reply ->
                    DatabaseSummary(
                        reply.database.name,
                        reply.database.created.toInstant(),
                        mapOf() // TODO
                    )
                }

    override fun connectDatabase(name: DatabaseName): Connection =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            connections[name]?.let {
                return it
            }
            val newConnection = ConnectionImpl(name, cacheSize)
            newConnection.start()
            connections[name] = newConnection
            newConnection
        }

    override fun shutdown() {
        active.set(false)
        connections.forEach { (name, connection) ->
            removeConnection(name)
            connection.shutdown()
        }
    }

    override fun shutdownNow() {
        active.set(false)
        connections.forEach { (name, connection) ->
            removeConnection(name)
            connection.shutdownNow()
        }
    }

    private fun removeConnection(database: DatabaseName) =
        connections.remove(database)

    private inner class ConnectionImpl(
        override val database: DatabaseName,
        eventCacheSize: Long,
    ) : Connection {
        private val connectionScope = CoroutineScope(Dispatchers.Default)
        private val latestRevision: AtomicReference<DatabaseSummary> = AtomicReference()

        private val dbCacheChannel = channelBuilder.build()
        private val dbCache: AsyncCache<DatabaseRevision, DatabaseSummary> =
            Caffeine.newBuilder()
                .maximumSize(eventCacheSize.div(EVENTS_TO_DATABASE_CACHE_SIZE_RATIO))
                .buildAsync()

        private val eventCacheChannel = channelBuilder.build()
        private val eventCache: AsyncLoadingCache<EventId, CloudEvent> =
            Caffeine.newBuilder()
                .maximumWeight(eventCacheSize * AVERAGE_EVENT_WEIGHT)
                .weigher<EventId, CloudEvent> { _, event -> event.data?.toBytes()?.size ?: EMPTY_EVENT_WEIGHT }
                .buildAsync(EventLoader(eventCacheChannel, database))

        private val grpcClientChannel = channelBuilder.build()
        private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(grpcClientChannel)

        fun start() {
            connectionScope.launch {
                grpcClient.connect(
                    DatabaseRequest.newBuilder()
                        .setName(database)
                        .build()
                ).collect { reply ->
                    when (reply.resultCase) {
                        DatabaseReply.ResultCase.DATABASE -> {
                            val summary = reply.database.toDomain()
                            setLatestRevision(summary)
                            dbCache.get(summary.revision) { _ -> summary }
                        }

                        DatabaseReply.ResultCase.NOT_FOUND -> {
                            shutdownNow()
                            throw DatabaseNotFoundError(database)
                        }

                        else -> throw SerializationError("Result not set")
                    }
                }
            }
        }

        override suspend fun transact(events: List<EventProposal>): Batch =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else {
                if (events.isEmpty())
                    throw NoEventsProvidedError

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
                        throw InvalidDatabaseNameError(result.invalidDatabaseNameError.name)

                    TransactBatchReply.ResultCase.NO_EVENTS_PROVIDED_ERROR ->
                        throw NoEventsProvidedError

                    TransactBatchReply.ResultCase.INVALID_EVENTS_ERROR ->
                        throw InvalidEventsError(
                            result.invalidEventsError.invalidEventsList.map { invalidEvent ->
                                invalidEvent.toDomain()
                            }
                        )

                    TransactBatchReply.ResultCase.DATABASE_NOT_FOUND_ERROR ->
                        throw DatabaseNotFoundError(result.databaseNotFoundError.name)

                    TransactBatchReply.ResultCase.STREAM_STATE_CONFLICT_ERROR ->
                        throw StreamStateConflictsError(
                            result.streamStateConflictError.conflictsList.map {
                                it.toDomain()
                            }
                        )

                    else -> throw InternalServerError(result.internalServerError.message)
                }
            }

        override fun db(): Database =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else
                latestRevision.get().let { summary ->
                    DatabaseImpl(
                        summary.name,
                        summary.created,
                        summary.streamRevisions,
                    )
                }

        // TODO: validate revision > 0, and ensure
        //  all valid revisions return a valid database
        //  event fuzzy (currently throws NPE if not an exact match?)
        override suspend fun db(revision: DatabaseRevision): Database =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else {
                val summary = dbCache.get(revision) { _, executor ->
                    connectionScope.future(executor.asCoroutineDispatcher()) {
                        val summary = loadDatabase(grpcClient, database, revision)
                        setLatestRevision(summary)
                        summary
                    }
                }.await()
                DatabaseImpl(
                    summary.name,
                    summary.created,
                    summary.streamRevisions,
                )
            }

        override suspend fun sync(): Database =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else
                loadDatabase(grpcClient, database, LATEST_DATABASE_REVISION_SIGIL).let { summary ->
                        setLatestRevision(summary)
                        dbCache.get(summary.revision) { _ -> summary }
                        DatabaseImpl(
                            summary.name,
                            summary.created,
                            summary.streamRevisions,
                        )
                    }

        override fun log(): Flow<Batch> =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else
                grpcClient.databaseLog(
                    DatabaseLogRequest
                        .newBuilder()
                        .setDatabase(database)
                        .build()
                ).map { batch ->
                    when (batch.resultCase) {
                        DatabaseLogReply.ResultCase.BATCH -> {
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
                        }

                        DatabaseLogReply.ResultCase.DATABASE_NOT_FOUND ->
                            throw DatabaseNotFoundError(batch.databaseNotFound.name)

                        else ->
                            throw SerializationError("Result not set")
                    }
                }


        fun databaseCacheStats(): CacheStats = dbCache.synchronous().stats()
        fun eventCacheStats(): CacheStats = eventCache.synchronous().stats()

        override fun shutdown() {
            connectionScope.cancel()
            doToAllChannels {
                it.shutdown().awaitTermination(GRPC_CONNECTION_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
            }
            invalidateCaches()
            removeConnection(database)
        }

        override fun shutdownNow() {
            connectionScope.cancel()
            doToAllChannels {
                it.shutdownNow().awaitTermination(GRPC_CONNECTION_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
            }
            invalidateCaches()
            removeConnection(database)
        }

        private fun setLatestRevision(database: DatabaseSummary) {
            latestRevision.getAndUpdate { current ->
                if (current == null || (database.revision > current.revision))
                    database
                else
                    current
            }
        }

        private fun doToAllChannels(block: (ManagedChannel) -> Unit) =
            listOf(
                dbCacheChannel,
                eventCacheChannel,
                grpcClientChannel
            ).forEach(block)

        private fun invalidateCaches() {
            dbCache.synchronous().invalidateAll()
            eventCache.synchronous().invalidateAll()
        }

        private suspend fun getCachedEvent(eventId: EventId): CloudEvent? =
            eventCache[eventId].await()

        private inner class DatabaseImpl(
            override val name: DatabaseName,
            override val created: Instant,
            override val streamRevisions: Map<StreamName, StreamRevision>,
        ) : Database {
            private val streams = ConcurrentHashMap<StreamName, List<EventId>>(streamRevisions.size)

            override val revision: DatabaseRevision
                get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
                    acc + v
                }

            override fun stream(streamName: StreamName): Flow<CloudEvent> =
                if (!connectionScope.isActive)
                    throw ConnectionClosedException(this@ConnectionImpl)
                else {
                    val eventCount = streamRevisions[streamName]
                        ?: throw StreamNotFoundError(database, streamName)
                    val result = grpcClient.stream(
                        StreamRequest.newBuilder()
                            .setDatabase(database)
                            .setStream(streamName)
                            .build()
                    )
                    result.map { reply ->
                        when (reply.resultCase) {
                            StreamEventIdReply.ResultCase.EVENT_ID ->
                                eventCache[EventId.fromString(reply.eventId)].await()
                            StreamEventIdReply.ResultCase.STREAM_NOT_FOUND ->
                                throw StreamNotFoundError(
                                    reply.streamNotFound.database,
                                    reply.streamNotFound.stream,
                                )
                            else -> throw SerializationError("Result not set")
                        }
                    }.take(eventCount.toInt())
                }

            override fun subjectStream(
                streamName: StreamName,
                subjectName: StreamSubject
            ): Flow<CloudEvent> =
                if (!connectionScope.isActive)
                    throw ConnectionClosedException(this@ConnectionImpl)
                else {
                    TODO("Not yet implemented")
                }

            override suspend fun event(eventId: EventId): CloudEvent? =
                if (!connectionScope.isActive)
                    throw ConnectionClosedException(this@ConnectionImpl)
                else
                    getCachedEvent(eventId)

            // Use as Data Class
            operator fun component1() = name
            operator fun component2() = created
            operator fun component3() = streamRevisions

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as DatabaseImpl

                if (name != other.name) return false
                if (created != other.created) return false
                if (streamRevisions != other.streamRevisions) return false

                return true
            }

            override fun hashCode(): Int {
                var result = name.hashCode()
                result = 31 * result + created.hashCode()
                result = 31 * result + streamRevisions.hashCode()
                return result
            }

            override fun toString(): String {
                return "Database(name='$name', created=$created, streamRevisions=$streamRevisions, revision=$revision)"
            }
        }
    }

    companion object {
        @JvmStatic
        fun eventProposal(
            event: CloudEvent,
            streamName: StreamName,
            streamState: ProposedEventStreamState = StreamState.Any,
        ) = EventProposal(
            event,
            streamName,
            streamState,
        )

        @JvmStatic
        fun eventProposal(
            eventType: String,
            streamName: StreamName,
            streamState: ProposedEventStreamState = StreamState.Any,
            data: CloudEventData? = null,
            dataContentType: String? = null,
            dataSchema: URI? = null,
            subject: String? = null,
            extensions: List<CloudEventExtension> = listOf(),
        ): EventProposal {
            val builder = CloudEventBuilder.v1()
                .withId("1")
                .withSource(URI("edb:client"))
                .withType(eventType)
                .withData(data)
                .withDataContentType(dataContentType)
                .withDataSchema(dataSchema)
                .withSubject(subject)
            for (extension in extensions)
                builder.withExtension(extension)
            return EventProposal(
                builder.build(),
                streamName,
                streamState,
            )
        }
    }
}

internal suspend fun loadDatabase(
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
            throw DatabaseNotFoundError(result.notFound.name)

        else ->
            throw SerializationError("Result not set")
    }
}

internal class StreamLoader(
    channel: Channel,
    private val database: DatabaseName,
) : AsyncCacheLoader<StreamName, Flow<EventId>> {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)

    override fun asyncLoad(
        key: StreamName?,
        executor: Executor?
    ): CompletableFuture<out Flow<EventId>> = runBlocking(executor!!.asCoroutineDispatcher()) {
        future { fetchStreamEventIds(key!!) }
    }

    private fun fetchStreamEventIds(stream: StreamName): Flow<EventId> {
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
                    throw StreamNotFoundError(
                        reply.streamNotFound.database,
                        reply.streamNotFound.stream,
                    )

                else -> throw SerializationError("Result not set")
            }
        }
    }
}

internal class EventLoader(
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
                    throw EventNotFoundError(
                        reply.eventNotFound.database,
                        EventId.fromString(reply.eventNotFound.eventId),
                    )

                else -> throw SerializationError("Result not set")
            }
        }
        return eventMap
    }
}
