package com.evidentdb.client

import io.cloudevents.CloudEvent
import java.time.Instant
import java.util.concurrent.CompletableFuture

interface Client {
    /**
     * Creates a database, which serves as the unit of total
     * event ordering in an event-sourcing system.
     *
     * @param name The name of the database to create, must match
     *  """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$""".
     * @return `true` if database was created, `false` otherwise.
     * @throws InvalidDatabaseNameError
     * @throws DatabaseNameAlreadyExistsError
     * @throws InternalServerError
     * @throws SerializationError
     */
    fun createDatabase(name: DatabaseName): Boolean

    /**
     * Deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     * @throws InvalidDatabaseNameError
     * @throws DatabaseNotFoundError
     * @throws InternalServerError
     * @throws SerializationError
     */
    fun deleteDatabase(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases.
     *
     * @returns [Iterable] of [DatabaseSummary]
     */
    fun catalog(): Iterable<DatabaseSummary>

    /**
     * Returns a connection to a specific database. This method caches,
     * so subsequent calls for the same database name will return
     * the same connection.
     *
     * @param name the database name.
     * @return the [Connection], possibly cached.
     * @throws DatabaseNotFoundError
     * @throws SerializationError
     */
    fun connectDatabase(name: DatabaseName): Connection
}

/**
 * A connection to a specific database, used to [transact]
 * batches of events into the database, to read the resulting
 * transaction [log], as well as to obtain [Database] values to
 * query (via [db] and [sync]). A background server-push process
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
 * @property database The connected database.
 */
interface Connection {
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
    fun transact(events: List<EventProposal>): CompletableFuture<Batch>

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
    fun db(revision: DatabaseRevision): CompletableFuture<Database>

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
    fun sync(): CompletableFuture<Database>

    /**
     * Returns the transaction log of this database as an [Iterable]
     * of [Batch]es.
     *
     * @return an [Iterable] of [Batch]es in transaction order.
     * @throws DatabaseNotFoundError when this connection's database
     *  is no longer present on the server
     *  (callers should [shutdown] the connection in this case)
     * @throws SerializationError in rare cases of client-server serialization issues
     */
    fun log(): Iterable<Batch>
    // TODO: Allow range iteration from a start revision (fuzzy) and
    //  iterating from there (possibly to a (fuzzy) end revision)

    /**
     * Shuts down this connection while awaiting any in-flight requests to
     * complete. Invalidates all local caches. Removes this connection from
     * the parent client's connection cache.
     */
    fun shutdown()

    /**
     * Shuts down this connection immediately, not awaiting in-flight requests to
     * complete. Invalidates all local caches. Removes this connection from
     * the parent client's connection cache.
     */
    fun shutdownNow()
}

interface Database {
    val name: DatabaseName
    val created: Instant
    val revision: DatabaseRevision
    val streamRevisions: Map<StreamName, StreamRevision>

    /**
     * Returns a future bearing the [List] of [CloudEvent]s comprising
     * this stream as of this [Database]'s revision.
     *
     * @return [List] of [CloudEvent]s comprising this stream, in transaction order.
     * @throws StreamNotFoundError if stream is not found within database.
     */
    fun stream(streamName: StreamName): CompletableFuture<List<CloudEvent>>

    /**
     * Returns a future bearing the [List] of [CloudEvent]s comprising
     * this subject stream as of this [Database]'s revision, if both stream and
     * events for the given subject on that stream exist.
     *
     * @return [List] of [CloudEvent]s comprising this stream, in transaction order, if any.
     */
    fun subjectStream(
        streamName: StreamName,
        subjectName: StreamSubject
    ): CompletableFuture<List<CloudEvent>?>

    /**
     * Returns a future bearing the [CloudEvent] having the given ID, if it exists.
     *
     * @return a [CompletableFuture] bearing the [CloudEvent]? if it exists w/in this database.
     */
    fun event(eventId: EventId): CompletableFuture<CloudEvent?>
}

// val client = EvidentDB(ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext())
// client.createDatabase("foo") // => true
// val conn = client.connect("foo")
// val db1 = conn.db()
// db1.stream("my-stream") // => []
// val batch = conn.transactBatch(
//   listOf(EventProposal("my-stream", CloudEvent.newBuilder()....))
// ).get()
// val db2 = conn.db(batch.revision).get()
// db1.stream("my-stream").get() // => []
// db2.stream("my-stream").get() // => [CloudEvent()]