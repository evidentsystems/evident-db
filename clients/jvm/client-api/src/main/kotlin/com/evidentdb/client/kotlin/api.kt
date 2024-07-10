package com.evidentdb.client.kotlin

import com.evidentdb.client.*
import com.evidentdb.client.java.EvidentDb as EvidentDbJava
import io.cloudevents.CloudEvent
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.CompletableFuture
import com.evidentdb.client.java.Connection as ConnectionJava
import com.evidentdb.client.java.Database as DatabaseJava

interface EvidentDb: EvidentDbJava {
    /**
     * Creates a database, which serves as the unit of total
     * event ordering in an event-sourcing system.
     *
     * @param name The name of the database to create, must match
     *  """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$""".
     * @return `true` if database was created, `false` otherwise.
     * @throws StatusException on gRPC error
     */
    suspend fun createDatabaseAsync(name: DatabaseName): Boolean

    /**
     * Deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     * @throws StatusException on gRPC error
     */
    suspend fun deleteDatabaseAsync(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases as a [Flow].
     *
     * @returns a [Flow] of [Database].
     * @throws StatusException on gRPC error
     */
    fun fetchCatalogAsync(): Flow<DatabaseName>

    /**
     * Returns a connection to a specific database. This method caches,
     * so subsequent calls for the same database name will return
     * the same connection.
     *
     * @param name the database name.
     * @return the [Connection], possibly cached.
     * @throws StatusException on gRPC error
     */
    override fun connectDatabase(name: DatabaseName): Connection
}

/**
 * A connection to a specific database, used to [transactAsync]
 * batches of events into the database, to read the resulting
 * transaction [fetchLogFlow], as well as to obtain [Database] values to
 * query (via [db], [fetchLatestDb], and [fetchDbAsOfAsync]). A background
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
 * @property database The name of the connected database.
 */
interface Connection: ConnectionJava {
    /**
     * Atomically adds a batch of events to the database.
     *
     * @param batch The [BatchProposal] to add to the database.
     * @return the [Batch] transacted to the database
     * @throws StatusException when invalid data provided to method, when the transaction
     *  is aborted due to user-provided stream state constraints, and
     *  when this connection's database is no longer present on the server,
     *  in rare cases of server-side or client-server serialization issues respectively
     * */
    suspend fun transactAsync(batch: BatchProposal): Batch

    /**
     * Immediately returns the latest database value available locally.
     *
     * @return the [Database] value.
     * */
    override fun db(): Database

    /**
     * Returns a [CompletableFuture] bearing the next database revision
     * greater than or equal to the given revision based on request/response with server.
     * Getting the database from the returned future may block indefinitely if
     * revision is greater than the latest available on server, so callers should
     * use timeouts or similar when getting the value of the returned future.
     *
     * @param revision a [Revision]. Future will not complete until
     *  this revision is available on the server.
     * @return a [CompletableFuture] bearing the [Database] at the given revision
     * @throws StatusException propagated by the [CompletableFuture] when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     * */
    override fun fetchDbAsOf(revision: Revision): CompletableFuture<out Database>

    /**
     * Returns the next database revision greater than or equal to the given revision.
     * This function may suspend indefinitely if revision is greater than the latest
     * available on server, so callers should manage with timeouts or similar.
     *
     * @param revision a [Revision]. Future will not complete until
     *  this revision is available on the server.
     * @return the [Database] that includes the given revision
     * @throws StatusException when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     * */
    suspend fun fetchDbAsOfAsync(revision: Revision): Database

    /**
     * Returns a completable future bearing the latest database available on the
     * server as of this request's arrival.
     *
     * @return a [CompletableFuture] bearing the latest [Database] available on the server
     * @throws StatusException propagated by the [CompletableFuture] when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     */
    override fun fetchLatestDb(): CompletableFuture<out Database>

    /**
     * Returns the latest database available on the server as of this request's arrival.
     *
     * @return the latest [Database] available on the server.
     * @throws StatusException when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     */
    suspend fun fetchLatestDbAsync(): Database

    /**
     * Returns the transaction log of this database as a [Flow]
     * of [Batch]es.
     *
     * @return a [Flow] of [Batch]es in transaction order.
     * @throws StatusException when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     */
    fun fetchLogFlow(): Flow<Batch>
    // TODO: Allow range iteration from a start revision (fuzzy) and
    //  iterating from there (possibly to a (fuzzy) end revision)
}

/**
 * Databases represent a revision of the database identified by [name], i.e. a
 * point-in-time queryable basis for querying streams of events. Databases have value
 * semantics, can be compared for equality, etc.
 *
 * The clock state of a Database is its [revision], a monotonically increasing unsigned long
 * representing the count of events present in the database.
 *
 * Databases cache their stream state (eventIds) and draw on their connection's event cache.
 * After their parent client or connection is closed, all subsequent API method calls will
 * throw [ConnectionClosedException].
 */
interface Database: DatabaseJava {
    /**
     * Returns a [Flow] of [CloudEvent]s comprising
     * this stream as of this [Database]'s revision.
     *
     * @return [Flow] of [CloudEvent]s comprising this stream, in transaction order.
     * @throws StreamNotFoundError if stream is not found within database.
     */
    fun fetchStreamAsync(streamName: StreamName): Flow<CloudEvent>

    /**
     * Returns a [Flow] of [CloudEvent]s having the given subject and the given stream as
     * of this [Database]'s revision.
     *
     * @return [Flow] of [CloudEvent]s comprising this stream, in transaction order.
     */
    fun fetchSubjectStreamAsync(
        streamName: StreamName,
        subjectName: StreamSubject
    ): Flow<CloudEvent>

    /**
     * Returns a [Flow] of [CloudEvent]s having this subject across all streams as
     * of this [Database]'s revision.
     *
     * @return [Flow] of [CloudEvent]s comprising this stream, in transaction order.
     */
    fun fetchSubjectAsync(subjectName: StreamSubject): Flow<CloudEvent>

    /**
     * Returns a [Flow] of [CloudEvent]s having this event type across all streams as
     * of this [Database]'s revision.
     *
     * @return [Flow] of [CloudEvent]s having this event type, in transaction order.
     */
    fun fetchEventTypeAsync(eventType: EventType): Flow<CloudEvent>

    /**
     * Returns the [CloudEvent] having the given ID, if it exists.
     *
     * @return the [CloudEvent]? if it exists w/in this database.
     */
    suspend fun fetchEventByIdAsync(eventId: EventId): CloudEvent?
}
