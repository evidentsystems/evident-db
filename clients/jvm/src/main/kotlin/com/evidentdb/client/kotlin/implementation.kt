package com.evidentdb.client.kotlin

import arrow.core.toNonEmptyListOrNull
import com.evidentdb.client.*
import com.evidentdb.client.transfer.toDomain
import com.evidentdb.client.transfer.toInstant
import com.evidentdb.client.transfer.toTransfer
import com.evidentdb.service.v1.*
import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.stats.CacheStats
import io.cloudevents.CloudEvent
import io.cloudevents.protobuf.toDomain
import io.cloudevents.protobuf.toTransfer
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
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
import kotlin.math.max

internal const val DEFAULT_CACHE_SIZE: Long = 10_000
internal const val AVERAGE_EVENT_WEIGHT = 1000
internal const val EMPTY_EVENT_WEIGHT = 10
internal const val LATEST_DATABASE_REVISION_SIGIL = 0uL
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
class GrpcClient(private val channelBuilder: ManagedChannelBuilder<*>) : EvidentDbKt {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channelBuilder.build())
    private val connections = ConcurrentHashMap<DatabaseName, Connection>(10)
    private val cacheSizeReference = AtomicLong(DEFAULT_CACHE_SIZE)
    private val active = AtomicBoolean(true)

    private val isActive: Boolean
        get() = active.get()

    // TODO: make this configurable, perhaps via a Java property?
    var cacheSize: Long
        get() = cacheSizeReference.get()
        set(value) {
            cacheSizeReference.set(value)
        }

    override fun createDatabase(name: DatabaseName): Boolean =
        runBlocking { createDatabaseAsync(name) }

    override suspend fun createDatabaseAsync(name: DatabaseName): Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            val result = grpcClient.createDatabase(
                CreateDatabaseRequest.newBuilder()
                    .setName(name)
                    .build()
            )
            result.hasDatabase()
        }

    override fun deleteDatabase(name: DatabaseName): Boolean =
        runBlocking { deleteDatabaseAsync(name) }

    override suspend fun deleteDatabaseAsync(name: DatabaseName): Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            val result = grpcClient.deleteDatabase(
                DeleteDatabaseRequest.newBuilder()
                    .setName(name)
                    .build()
            )
            result.hasDatabase()
        }

    override fun fetchCatalog(): CloseableIterator<com.evidentdb.client.Database> =
        fetchCatalogAsync().asIterator()

    override fun fetchCatalogAsync(): Flow<com.evidentdb.client.Database> =
        if (!isActive)
            throw ClientClosedException(this)
        else
            grpcClient
                .catalog(CatalogRequest.getDefaultInstance())
                .map { reply -> reply.database.toDomain() }

    override fun connectDatabase(name: DatabaseName): Connection =
        if (!isActive) {
            throw ClientClosedException(this)
        } else {
            // Return from the cache, if available
            connections[name]?.let {
                return@connectDatabase it
            }
            // Otherwise, create, start, and cache a new connection
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
        private lateinit var subscriptionURI: URI
        private lateinit var created: Instant
        private val connectionScope = CoroutineScope(Dispatchers.Default)
        private val latestRevision: AtomicReference<DatabaseRevision> = AtomicReference(0uL)

        private val eventCacheChannel = channelBuilder.build()
        private val eventCache: AsyncLoadingCache<EventRevision, CloudEvent> =
            Caffeine.newBuilder()
                .maximumWeight(eventCacheSize * AVERAGE_EVENT_WEIGHT)
                .weigher<EventRevision, CloudEvent> { _, event -> event.data?.toBytes()?.size ?: EMPTY_EVENT_WEIGHT }
                .buildAsync(EventLoader(eventCacheChannel, database))

        private val grpcClientChannel = channelBuilder.build()
        private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(grpcClientChannel)

        fun start() {
            connectionScope.launch {
                // TODO: handle errors, reconnection, etc.
                grpcClient.connect(
                    ConnectRequest.newBuilder()
                        .setName(database)
                        .build()
                ).collect { reply ->
                    val summary = reply.database.toDomain()
                    subscriptionURI = summary.subscriptionURI
                    created = summary.created
                    latestRevision.set(summary.revision)
                }
            }
        }

        override fun transact(batch: BatchProposal): CompletableFuture<Batch> =
            connectionScope.future { transactAsync(batch) }

        override suspend fun transactAsync(batch: BatchProposal): Batch =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else {
                // Fail fast on empty batch, no need to round-trip
                if (batch.events.isEmpty())
                    throw IllegalArgumentException("Batch cannot be empty")

                val result = grpcClient.transactBatch(
                    TransactBatchRequest.newBuilder()
                        .setDatabase(database)
                        .addAllEvents(batch.events.map { it.toTransfer() })
                        .addAllConstraints(batch.constraints.map { it.toTransfer() })
                        .build()
                )
                val batch = result.batch.toDomain()
                maybeSetLatestRevision(batch.revision)

                // TODO: index accepted batch events into local cache
                eventCache.synchronous().putAll(
                    batch.events.fold(mutableMapOf()) { acc, event ->
                        acc[event.revision] = event.event
                        acc
                    }
                )

                batch
            }

        override fun db(): Database =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else
                DatabaseImpl(
                    database,
                    subscriptionURI,
                    created,
                    latestRevision.get(),
                )

        override fun fetchDbAsOf(revision: DatabaseRevision): CompletableFuture<Database> =
            connectionScope.future {
                fetchDbAsOfAsync(revision)
            }

        /**
         * May block while awaiting database revision on server
         */
        override suspend fun fetchDbAsOfAsync(revision: DatabaseRevision): Database =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else {
                val summary = grpcClient.databaseAtRevision(
                    DatabaseAtRevisionRequest.newBuilder()
                        .setName(database)
                        .setRevision(revision.toLong())
                        .build()
                ).database.toDomain()
                maybeSetLatestRevision(summary.revision)
                DatabaseImpl(
                    summary.name,
                    summary.subscriptionURI,
                    summary.created,
                    summary.revision,
                )
            }

        override fun fetchLatestDb(): CompletableFuture<Database> =
            connectionScope.future {
                fetchLatestDbAsync()
            }

        override suspend fun fetchLatestDbAsync(): Database =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else
                grpcClient.latestDatabase(
                    LatestDatabaseRequest.newBuilder()
                        .setName(database)
                        .build()
                ).database.toDomain().let { summary ->
                        maybeSetLatestRevision(summary.revision)
                        DatabaseImpl(
                            summary.name,
                            summary.subscriptionURI,
                            summary.created,
                            summary.revision,
                        )
                    }

        override fun fetchLog(): CloseableIterator<Batch> =
            fetchLogFlow().asIterator()

        override fun fetchLogFlow(): Flow<Batch> =
            if (!connectionScope.isActive)
                throw ConnectionClosedException(this)
            else
                grpcClient.databaseLog(
                    DatabaseLogRequest
                        .newBuilder()
                        .setName(database)
                        .build()
                ).map { batch ->
                    val batchSummary = batch.batch
                    val batchEvents = eventCache.getAll(
                        batchSummary.eventRevisionsList.map { it.toULong() }
                    ).await()
                    Batch(
                        database,
                        batchSummary.eventRevisionsList.map {
                            Event(
                                batchEvents[it.toULong()]!!
                            )
                        }.toNonEmptyListOrNull()!!,
                        batchSummary.timestamp.toInstant(),
                        batchSummary.basisRevision.toULong(),
                    )
                }

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

        private fun maybeSetLatestRevision(revision: DatabaseRevision) {
            latestRevision.getAndUpdate { current ->
                max(revision, current)
            }
        }

        private fun doToAllChannels(block: (ManagedChannel) -> Unit) =
            listOf(
                eventCacheChannel,
                grpcClientChannel
            ).forEach(block)

        private fun invalidateCaches() {
            eventCache.synchronous().invalidateAll()
        }

        private suspend fun getCachedEvent(eventRevision: EventRevision): CloudEvent? =
            eventCache[eventRevision].await()

        private inner class DatabaseImpl(
            override val name: DatabaseName,
            override val subscriptionURI: URI,
            override val created: Instant,
            override val revision: DatabaseRevision,
        ) : Database {
            override fun fetchStream(streamName: StreamName): CloseableIterator<CloudEvent> =
                fetchStreamAsync(streamName).asIterator()

            override fun fetchStreamAsync(streamName: StreamName): Flow<CloudEvent> =
                if (!connectionScope.isActive)
                    throw ConnectionClosedException(this@ConnectionImpl)
                else {
                    processStreamReplyFlow(
                        grpcClient.stream(
                            StreamRequest.newBuilder()
                                .setDatabase(database)
                                .setRevision(revision.toLong())
                                .setStream(streamName)
                                .build()
                        )
                    )
                }

            override fun fetchSubjectStream(
                streamName: StreamName,
                subjectName: StreamSubject
            ): CloseableIterator<CloudEvent> =
                fetchSubjectStreamAsync(streamName, subjectName).asIterator()

            override fun fetchSubjectStreamAsync(
                streamName: StreamName,
                subjectName: StreamSubject
            ): Flow<CloudEvent> =
                if (!connectionScope.isActive)
                    throw ConnectionClosedException(this@ConnectionImpl)
                else {
                    processStreamReplyFlow(
                        grpcClient.subjectStream(
                            SubjectStreamRequest.newBuilder()
                                .setDatabase(database)
                                .setStream(streamName)
                                .setSubject(subjectName)
                                .build()
                        )
                    )
                }

            override fun fetchSubject(subjectName: StreamSubject): CloseableIterator<CloudEvent> =
                fetchSubjectAsync(subjectName).asIterator()

            override fun fetchSubjectAsync(subjectName: StreamSubject): Flow<CloudEvent> =
                if (!connectionScope.isActive)
                    throw ConnectionClosedException(this@ConnectionImpl)
                else {
                    processStreamReplyFlow(
                        grpcClient.subject(
                            SubjectRequest.newBuilder()
                                .setDatabase(database)
                                .setRevision(revision.toLong())
                                .setSubject(subjectName)
                                .build()
                        )
                    )
                }

            override fun fetchEventType(eventType: EventType): CloseableIterator<CloudEvent> =
                fetchEventTypeAsync(eventType).asIterator()

            override fun fetchEventTypeAsync(eventType: EventType): Flow<CloudEvent> =
                if (!connectionScope.isActive)
                    throw ConnectionClosedException(this@ConnectionImpl)
                else {
                    processStreamReplyFlow(
                        grpcClient.eventType(
                            EventTypeRequest.newBuilder()
                                .setDatabase(database)
                                .setRevision(revision.toLong())
                                .setEventType(eventType)
                                .build()
                        )
                    )
                }

            private fun processStreamReplyFlow(
                streamReplyFlow: Flow<EventRevisionReply>
            ) = streamReplyFlow
                .map { it.revision.toULong() }
                .takeWhile { r -> r <= revision }
                .map { r -> eventCache[r].await() }


            override fun fetchEventById(eventId: EventId): CompletableFuture<CloudEvent?> =
                runBlocking { future { fetchEventByIdAsync(eventId) } }

            override suspend fun fetchEventByIdAsync(eventId: EventId): CloudEvent? =
                if (!connectionScope.isActive) {
                    throw ConnectionClosedException(this@ConnectionImpl)
                } else {
                    // TODO: build a cache/map of id -> revision for 2-step lookup?
                    grpcClient.eventById(
                        EventByIdRequest.newBuilder()
                            .setDatabase(database)
                            .setEventId(eventId)
                            .build()
                    ).event.toDomain()
                }

            // Use as Data Class
            operator fun component1() = name
            operator fun component2() = created
            operator fun component3() = revision

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as DatabaseImpl

                if (name != other.name) return false
                if (created != other.created) return false
                if (revision != other.revision) return false

                return true
            }

            override fun hashCode(): Int {
                var result = name.hashCode()
                result = 31 * result + created.hashCode()
                result = 31 * result + revision.hashCode()
                return result
            }

            override fun toString(): String {
                return "Database(name='$name', topic=$subscriptionURI, created=$created, streamRevisions=$revision, revision=$revision)"
            }
        }
    }
}

internal class EventLoader(
    channel: Channel,
    private val database: DatabaseName,
) : AsyncCacheLoader<EventRevision, CloudEvent> {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)

    override fun asyncLoad(
        key: EventRevision?,
        executor: Executor?
    ): CompletableFuture<CloudEvent?> = runBlocking(executor!!.asCoroutineDispatcher()) {
        future { fetchEvents(listOf(key!!))[key] }
    }

    override fun asyncLoadAll(
        keys: MutableSet<out EventRevision>?,
        executor: Executor?
    ): CompletableFuture<out Map<out EventRevision, CloudEvent>> =
        runBlocking(executor!!.asCoroutineDispatcher()) {
            future { fetchEvents(keys!!.toList()) }
        }

    private suspend fun fetchEvents(
        eventIds: List<EventRevision>
    ): Map<EventRevision, CloudEvent> {
        val result = grpcClient.events(
            EventByRevisionRequest.newBuilder()
                .setDatabase(database)
                .addAllEventRevisions(eventIds.map { it.toLong() })
                .build()
        )
        val eventMap = mutableMapOf<EventRevision, CloudEvent>()
        result.collect { reply ->
            val event = Event(reply.event.toDomain())
            eventMap[event.revision] = event.event
        }
        return eventMap
    }
}
