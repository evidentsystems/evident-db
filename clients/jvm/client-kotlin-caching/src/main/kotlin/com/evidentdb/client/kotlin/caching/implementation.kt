package com.evidentdb.client.kotlin.caching

import com.evidentdb.client.*
import com.evidentdb.client.core.EvidentDb as EvidentDbCore
import com.evidentdb.client.kotlin.EvidentDb
import com.evidentdb.client.kotlin.Connection
import com.evidentdb.client.kotlin.ConnectionState
import com.evidentdb.client.kotlin.Database
import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.stats.CacheStats
import io.cloudevents.CloudEvent
import io.grpc.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.ThreadSafe
import kotlin.math.max

internal const val DEFAULT_CACHE_SIZE: Long = 10_000
internal const val AVERAGE_EVENT_WEIGHT = 1000
internal const val EMPTY_EVENT_WEIGHT = 10

/**
 * This is the top-level entry point for an EvidentDB Kotlin client that caches Event data.
 * Use instances of this client to create and delete databases, show
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
class KotlinCachingClient(private val channelBuilder: ManagedChannelBuilder<*>) : EvidentDb {
    private val coreClient = EvidentDbCore(channelBuilder.build())
    private val connections = ConcurrentHashMap<DatabaseName, Connection>(10)
    private val cacheSizeReference = AtomicLong(DEFAULT_CACHE_SIZE)

    // Cache config

    // TODO: make this configurable, perhaps via a Java property?
    var cacheSize: Long
        get() = cacheSizeReference.get()
        set(value) {
            cacheSizeReference.set(value)
        }

    // Lifecycle
    init {
        // Required for all clients
        init()
    }

    val isActive: Boolean
        get() = coreClient.isActive


    override fun shutdown() {
        coreClient.shutdown()
        connections.forEach { (_, connection) ->
            connection.shutdown()
        }
    }

    override fun shutdownNow() {
        coreClient.shutdownNow()
        connections.forEach { (_, connection) ->
            connection.shutdownNow()
        }
    }

    private fun removeConnection(database: DatabaseName) =
        connections.remove(database)

    // Client

    override suspend fun createDatabase(name: DatabaseName): Boolean =
        coreClient.createDatabase(name)

    override suspend fun deleteDatabase(name: DatabaseName): Boolean =
        coreClient.deleteDatabase(name)

    override fun fetchCatalog(): Flow<DatabaseName> =
        coreClient.fetchCatalog()

    override fun connectDatabase(name: DatabaseName): Connection =
        // No guaranteed baseClient interaction here, so manually check if active
        if (!isActive) {
            throw ClientClosedException(this)
        } else {
            // Return from connection cache, if available
            connections[name]?.let {
                return@connectDatabase it
            }
            // Otherwise, create, start, and cache a new connection
            val newConnection = ConnectionImpl(name, cacheSize)
            connections[name] = newConnection
            newConnection
        }

    private inner class ConnectionImpl(
        override val databaseName: DatabaseName,
        eventCacheSize: Long,
    ) : Connection {
        private val coreClient = EvidentDbCore(channelBuilder.build())
        private val scope = CoroutineScope(Dispatchers.Default)
        private val state = AtomicReference(ConnectionState.DISCONNECTED)
        private val latestRevision = AtomicReference(0uL)

        private val eventLoader = EventLoader(channelBuilder.build(), databaseName)
        private val eventCache: AsyncLoadingCache<Revision, Event> =
            Caffeine.newBuilder()
                .maximumWeight(eventCacheSize * AVERAGE_EVENT_WEIGHT)
                .weigher<Revision, Event> { _, event -> event.data?.toBytes()?.size ?: EMPTY_EVENT_WEIGHT }
                .buildAsync(eventLoader)

        private val isActive
            get() = this@KotlinCachingClient.isActive && state.get() != ConnectionState.CLOSED

        private fun setLatestRevision(revision: Revision) {
            latestRevision.getAndUpdate { prev -> max(revision, prev) }
        }

        init {
            val started = CompletableFuture<Boolean>()
            scope.launch {
                while (isActive) {
                    coreClient
                        .subscribeDatabaseUpdates(databaseName)
                        .catch { e ->
                            if (e is StatusException) {
                                if (!started.isDone) {
                                    started.complete(true)
                                }
                                state.set(ConnectionState.CLOSED)
                            }
                        }
                        .collect { batchSummary ->
                            if (!started.isDone) {
                                started.complete(true)
                            }
                            state.set(ConnectionState.CONNECTED)
                            setLatestRevision(batchSummary.revision)
                        }
                    state.set(ConnectionState.DISCONNECTED)
                }
            }
            try {
                started.get(30, TimeUnit.SECONDS)
            } catch (e: TimeoutException) {
                state.set(ConnectionState.CLOSED)
            }
        }

        override fun shutdown() {
            state.set(ConnectionState.CLOSED)
            coreClient.shutdown()
            eventLoader.shutdown()
            scope.cancel()
            invalidateCaches()
            removeConnection(databaseName)
        }

        override fun shutdownNow() {
            state.set(ConnectionState.CLOSED)
            coreClient.shutdownNow()
            eventLoader.shutdownNow()
            scope.cancel()
            invalidateCaches()
            removeConnection(databaseName)
        }

        // Cache management

        fun eventCacheStats(): CacheStats = eventCache.synchronous().stats()

        private fun invalidateCaches() {
            eventCache.synchronous().invalidateAll()
        }

        private suspend fun getCachedEvent(eventRevision: Revision): Event? =
            eventCache[eventRevision].await()

        // API

        override suspend fun transact(
            events: List<CloudEvent>,
            constraints: List<BatchConstraint>
        ): Batch {
            // Fail fast on empty batch, no need to round-trip
            if (events.isEmpty())
                throw IllegalArgumentException("Batch cannot be empty")

            val acceptedBatch = coreClient.transact(databaseName, events, constraints)
            setLatestRevision(acceptedBatch.revision)

            // Cache returned batch to quickly read own writes w/out server round-trip
            eventCache.synchronous().putAll(
                    acceptedBatch.events.fold(mutableMapOf()) { acc, event ->
                        acc[event.revision] = event
                        acc
                    }
                )
            return acceptedBatch
        }

        override fun db(): Database =
            // No baseClient interaction here, so manually check if active
            if (!isActive)
                throw ConnectionClosedException(this)
            else
                DatabaseImpl(
                    databaseName,
                    latestRevision.get(),
                )

        /**
         * May block while awaiting database revision on server
         */
        override suspend fun awaitDb(revision: Revision): Database {
            val summary = coreClient.awaitDatabase(databaseName, revision)
            setLatestRevision(summary.revision)
            return DatabaseImpl(
                summary.name,
                summary.revision,
            )
        }

        override suspend fun fetchLatestDb(): Database =
            coreClient.fetchLatestDatabase(databaseName).let { summary ->
                setLatestRevision(summary.revision)
                DatabaseImpl(
                    summary.name,
                    summary.revision,
                )
            }

        // TODO: cache events along the way?
        override fun scanDatabaseLog(startAtRevision: Revision): Flow<Batch> =
            coreClient.scanDatabaseLogDetail(databaseName, startAtRevision)

        private inner class DatabaseImpl(
            override val name: DatabaseName,
            override val revision: Revision,
        ) : Database {
            // TODO: Cache stream revisions?
            override fun fetchStream(streamName: StreamName): Flow<Event> =
                coreClient.fetchEventRevisionsByStream(
                    databaseName,
                    revision,
                    streamName
                ).map { eventCache[it].await() }

            // TODO: Cache subject stream revisions?
            override fun fetchSubjectStream(
                streamName: StreamName,
                subjectName: StreamSubject
            ): Flow<Event> =
                coreClient.fetchEventRevisionsBySubjectAndStream(
                    databaseName,
                    revision,
                    streamName,
                    subjectName
                ).map { eventCache[it].await() }

            // TODO: Cache subject revisions?
            override fun fetchSubject(subjectName: StreamSubject): Flow<Event> =
                coreClient.fetchEventRevisionsBySubject(
                    databaseName,
                    revision,
                    subjectName
                ).map { eventCache[it].await() }

            // TODO: Cache event type revisions?
            override fun fetchEventType(eventType: EventType): Flow<Event> =
                coreClient.fetchEventRevisionsByType(
                    databaseName,
                    revision,
                    eventType
                ).map { eventCache[it].await() }

            // TODO: Cache event stream+eventId -> revision at the connection level?
            override suspend fun fetchEventById(
                streamName: StreamName,
                eventId: EventId
            ): Event? =
                coreClient.fetchEventById(
                    databaseName,
                    revision,
                    streamName,
                    eventId,
                )?.let { event ->
                    eventCache.put(event.revision, CompletableFuture.completedFuture(event))
                    event
                }

            // Use as Data Class
            operator fun component1() = name
            operator fun component2() = revision

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as DatabaseImpl

                if (name != other.name) return false
                if (revision != other.revision) return false

                return true
            }

            override fun hashCode(): Int {
                var result = name.hashCode()
                result = 31 * result + revision.hashCode()
                return result
            }

            override fun toString(): String {
                return "Database(name='$name', revision=$revision)"
            }
        }
    }
}

internal class EventLoader(
    channel: ManagedChannel,
    private val database: DatabaseName,
) : AsyncCacheLoader<Revision, Event>, Shutdown {
    private val coreClient = EvidentDbCore(channel)

    // Lifecycle

    override fun shutdown() {
        coreClient.shutdown()
    }

    override fun shutdownNow() {
        coreClient.shutdownNow()
    }

    // API

    override fun asyncLoad(
        key: Revision?,
        executor: Executor?
    ): CompletableFuture<Event?> = runBlocking(executor!!.asCoroutineDispatcher()) {
        future { fetchEvents(listOf(key!!))[key] }
    }

    override fun asyncLoadAll(
        keys: MutableSet<out Revision>?,
        executor: Executor?
    ): CompletableFuture<out Map<out Revision, Event>> =
        runBlocking(executor!!.asCoroutineDispatcher()) {
            future { fetchEvents(keys!!.toList()) }
        }

    private suspend fun fetchEvents(
        eventIds: List<Revision>
    ): Map<Revision, Event> {
        val eventMap = mutableMapOf<Revision, Event>()
        coreClient
            .fetchEventsByRevisions(database, eventIds)
            .collect { event ->
                eventMap[event.revision] = event
            }
        return eventMap
    }
}
