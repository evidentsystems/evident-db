package com.evidentdb.client.kotlin

import arrow.core.toNonEmptyListOrNull
import com.evidentdb.client.*
import com.evidentdb.client.transfer.toDomain
import com.evidentdb.client.transfer.toInstant
import com.evidentdb.client.transfer.toTransfer
import com.evidentdb.service.v1.*
import io.cloudevents.CloudEvent
import io.cloudevents.protobuf.toDomain
import io.cloudevents.protobuf.toTransfer
import io.grpc.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.ThreadSafe
import kotlin.math.max

internal const val GRPC_CONNECTION_SHUTDOWN_TIMEOUT = 30L

fun EvidentDb.Companion.kotlinClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDbKt =
    KotlinSimpleClient(channelBuilder)

enum class ConnectionState {
    DISCONNECTED,
    CONNECTED,
    CLOSED
}

interface SimpleClient: EvidentDbKt {
    val grpcClient: EvidentDbGrpcKt.EvidentDbCoroutineStub
    val isActive: Boolean
    val connections: MutableMap<DatabaseName, Connection>

    override fun createDatabase(name: DatabaseName): Boolean =
        runBlocking { createDatabaseAsync(name) }

    override suspend fun createDatabaseAsync(name: DatabaseName): Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            try {
                val result = grpcClient.createDatabase(
                    CreateDatabaseRequest.newBuilder()
                        .setName(name)
                        .build()
                )
                result.hasDatabase()
            } catch (e: StatusException) {
                if (e.status.code == Status.Code.ALREADY_EXISTS) {
                    false
                } else {
                    throw e
                }
            }
        }

    override fun deleteDatabase(name: DatabaseName): Boolean =
        runBlocking { deleteDatabaseAsync(name) }

    override suspend fun deleteDatabaseAsync(name: DatabaseName): Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            try {
                val result = grpcClient.deleteDatabase(
                    DeleteDatabaseRequest.newBuilder()
                        .setName(name)
                        .build()
                )
                result.hasDatabase()
            } catch (e: StatusException) {
                if (e.status.code == Status.Code.NOT_FOUND) {
                    false
                } else {
                    throw e
                }
            }
        }

    override fun fetchCatalog(): CloseableIterator<DatabaseName> =
        fetchCatalogAsync().asIterator()

    override fun fetchCatalogAsync(): Flow<DatabaseName> =
        if (!isActive)
            throw ClientClosedException(this)
        else
            grpcClient
                .catalog(CatalogRequest.getDefaultInstance())
                .map { reply -> reply.databaseName }
}

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
 * @constructor Main entry point for creating EvidentDB clients
 */
@ThreadSafe
class KotlinSimpleClient(private val channelBuilder: ManagedChannelBuilder<*>) : SimpleClient {
    override val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channelBuilder.build())
    override val connections = ConcurrentHashMap<DatabaseName, Connection>(10)
    private val active = AtomicBoolean(true)

    override val isActive: Boolean
        get() = active.get()

    private fun removeConnection(database: DatabaseName) =
        connections.remove(database)

    override fun connectDatabase(name: DatabaseName): Connection =
        if (!isActive) {
            throw ClientClosedException(this)
        } else {
            // Return from storage, if available
            connections[name]?.let {
                return@connectDatabase it
            }
            // Otherwise, create, start, and store a new connection
            val newConnection = ConnectionImpl(name)
            connections[name] = newConnection
            newConnection
        }

    override fun shutdown() {
        if (isActive) {
            active.set(false)
            connections.forEach { (name, connection) ->
                removeConnection(name)
                connection.shutdown()
            }
        }
    }

    override fun shutdownNow() {
        if (isActive) {
            active.set(false)
            connections.forEach { (name, connection) ->
                removeConnection(name)
                connection.shutdownNow()
            }
        }
    }

    private inner class ConnectionImpl(
        override val database: DatabaseName
    ) : Connection {
        private val scope = CoroutineScope(Dispatchers.Default)
        private val state = AtomicReference(ConnectionState.DISCONNECTED)
        private val latestRevision = AtomicReference(0uL)

        private val grpcClientChannel = channelBuilder.build()
        private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(grpcClientChannel)

        private val isActive
            get() = state.get() != ConnectionState.CLOSED

        init {
            val started = CompletableFuture<Boolean>()
            scope.launch {
                while (isActive) {
                    try {
                        grpcClient.connect(
                            ConnectRequest.newBuilder()
                                .setName(database)
                                .build()
                        ).collect { reply ->
                            if (!started.isDone) {
                                started.complete(true)
                            }
                            state.set(ConnectionState.CONNECTED)
                            val summary = reply.database.toDomain()
                            latestRevision.set(summary.revision)
                        }
                        state.set(ConnectionState.DISCONNECTED)
                    } catch (e: StatusException) {
                        if (!started.isDone) {
                            started.complete(true)
                        }
                        state.set(ConnectionState.CLOSED)
                    }
                }
            }
            try {
                started.get(30, TimeUnit.SECONDS)
            } catch (e: TimeoutException) {
                state.set(ConnectionState.CLOSED)
            }
        }

        override fun transact(batch: BatchProposal): CompletableFuture<Batch> =
            scope.future { transactAsync(batch) }

        override suspend fun transactAsync(batch: BatchProposal): Batch =
            if (!isActive)
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
                val acceptedBatch = result.batch.toDomain()
                maybeSetLatestRevision(acceptedBatch.revision)

                acceptedBatch
            }

        override fun db(): Database =
            if (!isActive)
                throw ConnectionClosedException(this)
            else
                DatabaseImpl(
                    database,
                    latestRevision.get(),
                )

        override fun fetchDbAsOf(revision: Revision): CompletableFuture<Database> =
            scope.future {
                fetchDbAsOfAsync(revision)
            }

        /**
         * May block while awaiting database revision on server
         */
        override suspend fun fetchDbAsOfAsync(revision: Revision): Database =
            if (!isActive)
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
                    summary.revision,
                )
            }

        override fun fetchLatestDb(): CompletableFuture<Database> =
            scope.future {
                fetchLatestDbAsync()
            }

        override suspend fun fetchLatestDbAsync(): Database =
            if (!isActive)
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
                            summary.revision,
                        )
                    }

        override fun fetchLog(): CloseableIterator<Batch> =
            fetchLogFlow().asIterator()

        override fun fetchLogFlow(): Flow<Batch> =
            if (!isActive)
                throw ConnectionClosedException(this)
            else
                grpcClient.databaseLog(
                    DatabaseLogRequest
                        .newBuilder()
                        .setName(database)
                        .build()
                ).map { batch ->
                    val batchSummary = batch.batch
                    val eventRevisions = (batchSummary.basis + 1).toULong()..batchSummary.revision.toULong()
                    // fetch events by revision here
                    val batchEvents = fetchEvents(eventRevisions.toList())
                    Batch(
                        database,
                        batchSummary.basis.toULong(),
                        batchEvents.map { Event(it) }.toNonEmptyListOrNull()!!,
                        batchSummary.timestamp.toInstant(),
                    )
                }

        override fun shutdown() {
            state.set(ConnectionState.CLOSED)
            scope.cancel()
            grpcClientChannel.shutdown().awaitTermination(GRPC_CONNECTION_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
            removeConnection(database)
        }

        override fun shutdownNow() {
            state.set(ConnectionState.CLOSED)
            scope.cancel()
            grpcClientChannel.shutdownNow().awaitTermination(GRPC_CONNECTION_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
            removeConnection(database)
        }

        private fun maybeSetLatestRevision(revision: Revision) =
            latestRevision.getAndUpdate { current ->
                max(revision, current)
            }

         private suspend fun fetchEvents(
            revisions: List<Revision>
        ): List<CloudEvent> = grpcClient.events(
            EventByRevisionRequest.newBuilder()
                .setDatabase(database)
                .addAllEventRevisions(revisions.map { it.toLong() })
                .build()
        )
            .map { it.event.toDomain() }
            .toList()

        private suspend fun fetchEvent(revision: Revision): CloudEvent =
            fetchEvents(listOf(revision)).first()

        private inner class DatabaseImpl(
            override val name: DatabaseName,
            override val revision: Revision,
        ) : Database {
            override fun fetchStream(streamName: StreamName): CloseableIterator<CloudEvent> =
                fetchStreamAsync(streamName).asIterator()

            override fun fetchStreamAsync(streamName: StreamName): Flow<CloudEvent> =
                if (!isActive)
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
                if (!isActive)
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
                if (!isActive)
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
                if (!isActive)
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
                .map { r -> fetchEvent(r)!! }

            override fun fetchEventById(eventId: EventId): CompletableFuture<CloudEvent?> =
                runBlocking { future { fetchEventByIdAsync(eventId) } }

            override suspend fun fetchEventByIdAsync(eventId: EventId): CloudEvent? =
                if (!isActive) {
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
