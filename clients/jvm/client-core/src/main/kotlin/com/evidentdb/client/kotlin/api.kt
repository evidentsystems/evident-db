package com.evidentdb.client.kotlin

import com.evidentdb.client.*
import io.cloudevents.CloudEvent
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import javax.annotation.concurrent.ThreadSafe

/**
 * This is the EvidentDB Kotlin client.
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
interface EvidentDb: Shutdown, ClientHelpers {
    /**
     * Creates a database, which serves as the unit of total
     * event ordering in an event-sourcing system.
     *
     * @param name The name of the database to create, must match
     *  """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$""".
     * @return `true` if database was created, `false` otherwise.
     * @throws StatusException on gRPC error
     */
    suspend fun createDatabase(name: DatabaseName): Boolean

    /**
     * Deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     * @throws StatusException on gRPC error
     */
    suspend fun deleteDatabase(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases as a [Flow].
     *
     * @returns a [Flow] of [Database].
     * @throws StatusException on gRPC error
     */
    fun fetchCatalog(): Flow<DatabaseName>

    /**
     * Returns a connection to a specific database. This method caches,
     * so subsequent calls for the same database name will return
     * the same connection.
     *
     * @param name the database name.
     * @return the [Connection], possibly cached.
     * @throws StatusException on gRPC error
     */
    fun connectDatabase(name: DatabaseName): Connection
}

enum class ConnectionState {
    DISCONNECTED,
    CONNECTED,
    CLOSED
}

/**
 * A connection to a specific database, used to [transact]
 * batches of events into the database, to read the resulting
 * transaction [scanDatabaseLog], as well as to obtain [Database] values to
 * query (via [db], [fetchLatestDb], and [awaitDb]). A background
 * server-push process keeps the connection informed as new database revisions
 * are produced on the server (i.e. due to any clients transacting event batches).
 *
 * Connections are thread-safe and long-lived, and do not follow
 * the acquire-use-release pattern. When a program is finished
 * accessing a database (e.g. at program termination), connections
 * may be cleanly [shutdown] (allowing in-flight requests to complete)
 * or urgently [shutdownNow] (cancelling in-flight requests). After shutdown,
 * all subsequent API method calls will throw [ConnectionClosedException].
 **
 * @property databaseName The name of the connected database.
 */
interface Connection: Shutdown {
    val databaseName: DatabaseName

    /**
     * Atomically adds a batch of events to the database.
     *
     * @param events The [List] of [Event]s to add to the database.
     * @param constraints The [List] of [BatchConstraint]s that the event batch is conditional upon.
     * @return the [Batch] transacted to the database
     * @throws StatusException when invalid data provided to method; when the transaction
     *  is aborted due to user-provided stream state constraints;
     *  when this connection's database is no longer present on the server; or
     *  in rare cases of server-side or client-server serialization issues
     * */
    suspend fun transact(
        events: List<CloudEvent>,
        constraints: List<BatchConstraint>,
    ): Batch

    /**
     * Immediately returns the latest database value available locally.
     *
     * @return the [Database] value.
     * */
    fun db(): Database

    /**
     * Returns the latest database available on the server as of this inquiry.
     *
     * @return the latest [Database] available on the server.
     * @throws StatusException when this connection's database is no longer present
     *  on the server or for network or server errors
     */
    suspend fun fetchLatestDb(): Database

    /**
     * Returns the next database whose revision is greater than or equal to the given revision.
     * This function may suspend indefinitely if revision is greater than the latest
     * available on server, so callers should manage with timeouts or similar.
     *
     * @param revision a [Revision]. This function will suspend until
     *  this revision is available on the server.
     * @return the [Database] that includes the given revision
     * @throws StatusException when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     * */
    suspend fun awaitDb(revision: Revision): Database

    /**
     * Returns the transaction log of this database as a [Flow]
     * of [Batch]es.
     *
     * @return a [Flow] of [Batch]es in transaction order.
     * @throws StatusException when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     */
    fun scanDatabaseLog(startAtRevision: Revision = 0uL): Flow<Batch>
}

/**
 * Databases represent point-in-time of the database identified by its [name], i.e. a
 * point-in-time queryable basis for querying streams of events. Databases have value
 * semantics, can be compared for equality, etc.
 *
 * The clock state of a Database is its [revision], a monotonically increasing unsigned long
 * representing the count of events present in the database.
 *
 * After a database's parent client or connection is closed, all subsequent API method calls will
 * throw [ConnectionClosedException].
 */
interface Database {
    val name: DatabaseName
    val revision: Revision

    /**
     * Returns a [Flow] of [Event]s comprising
     * this stream as of this [Database]'s revision.
     *
     * @return [Flow] of [Event]s comprising this stream, in transaction order.
     * @throws StatusException for network or server errors
     */
    fun fetchStream(streamName: StreamName): Flow<Event>

    /**
     * Returns a [Flow] of [Event]s having the given subject and the given stream as
     * of this [Database]'s revision.
     *
     * @return [Flow] of [Event]s comprising this stream, in transaction order.
     */
    fun fetchSubjectStream(
        streamName: StreamName,
        subjectName: StreamSubject
    ): Flow<Event>

    /**
     * Returns a [Flow] of [Event]s having this subject across all streams as
     * of this [Database]'s revision.
     *
     * @return [Flow] of [Event]s comprising this stream, in transaction order.
     */
    fun fetchSubject(subjectName: StreamSubject): Flow<Event>

    /**
     * Returns a [Flow] of [Event]s having this event type across all streams as
     * of this [Database]'s revision.
     *
     * @return [Flow] of [Event]s having this event type, in transaction order.
     */
    fun fetchEventType(eventType: EventType): Flow<Event>

    /**
     * @return the [Event] if it exists w/in this database.
     */
    suspend fun fetchEventById(streamName: StreamName, eventId: EventId): Event?
}
