package com.evidentdb.client.kotlin

import com.evidentdb.client.*
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.CompletableFuture
import com.evidentdb.client.java.Connection as ConnectionJava
import com.evidentdb.client.java.Database as DatabaseJava

/**
 * A connection to a specific database, used to [transactAsync]
 * batches of events into the database, to read the resulting
 * transaction [fetchLogFlow], as well as to obtain [Database] values to
 * query (via [db], [fetchLatestDb], and [fetchDbAsOfAsync]). A background server-push process
 * keeps the connection informed as new database revisions are produced
 * on the server (i.e. due to any clients transacting event batches).
 *
 * Connections are thread-safe and long-lived, and do not follow
 * the acquire-use-release pattern. When a program is finished
 * accessing a database (e.g. at program termination), connections
 * may be cleanly [shutdown] (allowing in-flight requests to complete)
 * or urgently [shutdownNow] (cancelling in-flight requests). After shutdown,
 * all subsequent API method calls will throw [ConnectionClosedException].
 *
 * Connections cache database revisions, stream state (event ids), and events,
 * so that database values can often serve queries from local state.
 *
 * @property database The connected database.
 */
interface Connection: ConnectionJava {
    /**
     * Atomically adds a batch of events to the database.
     *
     * @param events The [EventProposal]s to atomically add to the database.
     * @return the [Batch] transacted to the database
     * @throws IllegalArgumentException subtypes [InvalidDatabaseNameError],
     *  [NoEventsProvidedError], [InvalidEventsError] when invalid data provided to method
     * @throws IllegalStateException subtypes [StreamStateConflictsError] when the transaction
     *  is aborted due to user-provided stream state constraints, and [DatabaseNotFoundError]
     *  when this connection's database is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws RuntimeException subtypes [InternalServerError] and [SerializationError]
     *  in rare cases of server-side or client-server serialization issues respectively
     * */
    suspend fun transactAsync(batch: BatchProposal): Batch

    /**
     * Immediately returns the latest database value available locally.
     *
     * @return the Database value.
     * */
    override fun db(): Database

    /**
     * Returns a completable future bearing the next database revision
     * greater than or equal to the given revision. Getting the database from
     * the returned future may block indefinitely if revision is greater
     * than the latest available on server, so callers should use timeouts or
     * similar when getting the value of the returned future.
     *
     * @param revision a DatabaseRevision (Long). Future will not complete until
     *  this revision is available on the server.
     * @return a [CompletableFuture] bearing the [Database] at the given revision
     * @throws DatabaseNotFoundError conveyed by the [CompletableFuture] when this
     *  connection's database is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws SerializationError conveyed by the [CompletableFuture] in rare cases
     *  of client-server serialization issues
     * */
    override fun fetchDbAsOf(revision: DatabaseRevision): CompletableFuture<out Database>

    /**
     * Returns the next database revision greater than or equal to the given revision.
     * This function may suspend indefinitely if revision is greater than the latest
     * available on server, so callers should manage with timeouts or similar.
     *
     * @param revision a DatabaseRevision (Long). Function with suspend until
     *  this revision is available on the server.
     * @return the [Database] at the given revision
     * @throws DatabaseNotFoundError when this connection's database is no
     *  longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws SerializationError in rare cases of client-server serialization issues
     * */
    suspend fun fetchDbAsOfAsync(revision: DatabaseRevision): Database

    /**
     * Returns a completable future bearing the latest database available on the
     * server as of this request's arrival.
     *
     * @return a [CompletableFuture] bearing the latest [Database] available on the server.
     * @throws DatabaseNotFoundError conveyed by the [CompletableFuture] when this
     *  connection's database is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws SerializationError conveyed by the [CompletableFuture] in rare cases
     *  of client-server serialization issues
     */
    override fun fetchLatestDb(): CompletableFuture<out Database>

    /**
     * Returns the latest database available on the server as of this request's arrival.
     *
     * @return the latest [Database] available on the server.
     * @throws DatabaseNotFoundError when this
     *  connection's database is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws SerializationError in rare cases of client-server serialization issues
     */
    suspend fun fetchLatestDbAsync(): Database

    /**
     * Returns the transaction log of this database as a [Flow]
     * of [Batch]es.
     *
     * @return a [Flow] of [Batch]es in transaction order.
     * @throws DatabaseNotFoundError when this connection's database
     *  is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws SerializationError in rare cases of client-server serialization issues
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
