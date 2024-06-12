package com.evidentdb.client.java

import com.evidentdb.client.*
import io.cloudevents.CloudEvent
import java.util.concurrent.CompletableFuture

/**
 * A connection to a specific database, used to [transact]
 * batches of events into the database, to read the resulting
 * transaction [fetchLog], as well as to obtain [Database] values to
 * query (via [db], [fetchLatestDb], and [fetchDbAsOf]). A background server-push process
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
interface Connection: Lifecycle {
    val database: DatabaseName

    /**
     * Atomically adds a batch of events to the database.
     *
     * @param events The [EventProposal]s to atomically add to the database.
     * @return a [CompletableFuture] conveying the [Batch] transacted to the database
     * @throws IllegalArgumentException subtypes [InvalidDatabaseNameError],
     *  [NoEventsProvidedError], [InvalidEventsError] when invalid data provided to method
     * @throws IllegalStateException subtypes [StreamStateConflictsError] when the transaction
     *  is aborted due to user-provided stream state constraints, and [DatabaseNotFoundError]
     *  when this connection's database is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws RuntimeException subtypes [InternalServerError] and [SerializationError]
     *  in rare cases of server-side or client-server serialization issues respectively
     * */
    fun transact(batch: BatchProposal): CompletableFuture<out Batch>

    /**
     * Immediately returns the latest database value available locally.
     *
     * @return the Database value.
     * */
    fun db(): Database

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
    fun fetchDbAsOf(revision: Revision): CompletableFuture<out Database>

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
    fun fetchLatestDb(): CompletableFuture<out Database>

    /**
     * Returns the transaction log of this database as a [CloseableIterator]
     * of [Batch]es.
     *
     * Callers must [CloseableIterator.close] after using the returned iterator, whether
     * the iterator is consumed to completion or not.
     *
     * @return a [CloseableIterator] of [Batch]es in transaction order.
     * @throws DatabaseNotFoundError when this connection's database
     *  is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws SerializationError in rare cases of client-server serialization issues
     */
    fun fetchLog(): CloseableIterator<Batch>
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
interface Database {
    val name: DatabaseName
    val revision: Revision

    /**
     * Returns a [CloseableIterator] of [CloudEvent]s comprising
     * this stream as of this [Database]'s revision.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [CloudEvent]s comprising this stream, in transaction order.
     * @throws StreamNotFoundError if stream is not found within database.
     */
    fun fetchStream(streamName: StreamName): CloseableIterator<CloudEvent>

    /**
     * Returns a [CloseableIterator] of [CloudEvent]s comprising this subject stream as
     * of this [Database]'s revision, if both stream and events for the given subject
     * on that stream exist.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [CloudEvent]s comprising this stream, if any, in transaction order.
     */
    fun fetchSubjectStream(
        streamName: StreamName,
        subjectName: StreamSubject
    ): CloseableIterator<CloudEvent>


    /**
     * Returns a [CloseableIterator] of [CloudEvent]s comprising this subject across all streams as
     * of this [Database]'s revision.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [CloudEvent]s comprising this subject, if any, in transaction order.
     */
    fun fetchSubject(
        subjectName: StreamSubject
    ): CloseableIterator<CloudEvent>

    /**
     * Returns a [CloseableIterator] of [CloudEvent]s having the given event type across all streams
     * as of this [Database]'s revision.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [CloudEvent]s having the given event type, if any, in transaction order.
     */
    fun fetchEventType(
        eventType: EventType
    ): CloseableIterator<CloudEvent>

    /**
     * Returns a future bearing the [CloudEvent] having the given ID, if it exists.
     *
     * @return a [CompletableFuture] bearing the [CloudEvent]? if it exists w/in this database.
     */
    fun fetchEventById(eventId: EventId): CompletableFuture<out CloudEvent?>
}
