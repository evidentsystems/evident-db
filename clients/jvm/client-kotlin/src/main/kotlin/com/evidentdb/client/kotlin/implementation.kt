package com.evidentdb.client.kotlin

import com.evidentdb.client.*
import com.evidentdb.client.core.EvidentDb as EvidentDbCore
import io.cloudevents.CloudEvent
import io.grpc.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.ThreadSafe
import kotlin.math.max

/**
 * This is the top-level entry point for the basic (non-caching) EvidentDB Kotlin client.
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
 * @constructor Main entry point for creating EvidentDB clients
 */
@ThreadSafe
class KotlinClient(private val channelBuilder: ManagedChannelBuilder<*>) : EvidentDb {
    private val coreClient = EvidentDbCore(channelBuilder.build())
    private val connections = ConcurrentHashMap<DatabaseName, Connection>(10)

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
        if (!isActive) {
            throw ClientClosedException(this)
        } else {
            // Return from connection cache, if available
            connections[name]?.let {
                return@connectDatabase it
            }
            // Otherwise, create, start, and store a new connection
            val newConnection = ConnectionImpl(name)
            connections[name] = newConnection
            newConnection
        }

    private inner class ConnectionImpl(
        override val databaseName: DatabaseName
    ) : Connection {
        private val coreClient = EvidentDbCore(channelBuilder.build())
        private val scope = CoroutineScope(Dispatchers.Default)
        private val state = AtomicReference(ConnectionState.DISCONNECTED)
        private val latestRevision = AtomicReference(0uL)

        // Lifecycle

        private val isActive
            get() = this@KotlinClient.isActive && state.get() != ConnectionState.CLOSED

        private fun setLatestRevision(revision: Revision) {
            latestRevision.getAndUpdate { prev -> max(revision, prev) }
        }

        // Start the log tail subscription loop
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
            scope.cancel()
            removeConnection(databaseName)
        }

        override fun shutdownNow() {
            state.set(ConnectionState.CLOSED)
            coreClient.shutdownNow()
            scope.cancel()
            removeConnection(databaseName)
        }

        override suspend fun transact(
            events: List<CloudEvent>,
            constraints: List<BatchConstraint>
        ): Batch {
            // Fail fast on empty batch, no need to round-trip
            if (events.isEmpty())
                throw IllegalArgumentException("Batch cannot be empty")

            val acceptedBatch = coreClient.transact(databaseName, events, constraints)
            setLatestRevision(acceptedBatch.revision)

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

        override fun scanDatabaseLog(startAtRevision: Revision): Flow<Batch> =
            coreClient.scanDatabaseLogDetail(databaseName, startAtRevision)

        private inner class DatabaseImpl(
            override val name: DatabaseName,
            override val revision: Revision,
        ) : Database {
            override fun fetchStream(streamName: StreamName): Flow<Event> =
                coreClient.fetchEventsByStream(
                    databaseName,
                    revision,
                    streamName
                )

            override fun fetchSubjectStream(
                streamName: StreamName,
                subjectName: StreamSubject
            ): Flow<Event> =
                coreClient.fetchEventsBySubjectAndStream(
                    databaseName,
                    revision,
                    streamName,
                    subjectName,
                )

            override fun fetchSubject(subjectName: StreamSubject): Flow<Event> =
                coreClient.fetchEventsBySubject(
                    databaseName,
                    revision,
                    subjectName,
                )

            override fun fetchEventType(eventType: EventType): Flow<Event> =
                coreClient.fetchEventsByType(
                    databaseName,
                    revision,
                    eventType,
                )

            override suspend fun fetchEventById(streamName: StreamName, eventId: EventId): Event? =
                coreClient.fetchEventById(
                    databaseName,
                    revision,
                    streamName,
                    eventId,
                )

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
