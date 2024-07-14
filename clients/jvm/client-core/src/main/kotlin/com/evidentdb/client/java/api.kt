package com.evidentdb.client.java

import com.evidentdb.client.*
import io.cloudevents.CloudEvent

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.some
import io.grpc.StatusException
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.ThreadSafe
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

interface EvidentDb: Shutdown, ClientHelpers {
    /**
     * Synchronously creates a database, which serves as the unit of total
     * event ordering in an event-sourcing system.
     *
     * @param name The name of the database to create, must match
     *  """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$""".
     * @return `true` if database was created, `false` otherwise.
     * @throws StatusException on gRPC error
     */
    fun createDatabase(name: DatabaseName): Boolean

    /**
     * Synchronously deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     * @throws StatusException on gRPC error
     */
    fun deleteDatabase(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases as an iterator.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [AutoCloseable.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not (e.g. using try-with-resources or similar).
     *
     * @returns a [CloseableIterator] of [Database].
     */
    fun fetchCatalog(): CloseableIterator<DatabaseName>

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
    fun transact(
        events: List<CloudEvent>,
        constraints: List<BatchConstraint>,
    ): CompletableFuture<out Batch>

    /**
     * Immediately returns the latest database value available locally.
     *
     * @return the [Database] value.
     * */
    fun db(): Database

    /**
     * Returns a completable future bearing the latest database available on the
     * server as of this inquiry.
     *
     * @return a [CompletableFuture] bearing the latest [Database] available on the server.
     * @throws StatusException conveyed by [CompletableFuture] when this connection's
     *  database is no longer present on the server or for network or server errors
     */
    fun fetchLatestDb(): CompletableFuture<out Database>

    /**
     * Returns a completable future bearing the next database revision
     * greater than or equal to the given revision. Getting the database from
     * the returned future may block indefinitely if revision is greater
     * than the latest available on server, so callers should use timeouts or
     * similar when getting the value of the returned future.
     *
     * @param revision a [Revision]. Future will not complete until
     *  this revision is available on the server.
     * @return a [CompletableFuture] bearing the [Database] at the given revision
     * @throws StatusException when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     * */
    fun awaitDb(revision: Revision): CompletableFuture<out Database>

    /**
     * Returns the transaction log of this database as a [CloseableIterator]
     * of [Batch]es.
     *
     * Callers must [CloseableIterator.close] after using the returned iterator, whether
     * the iterator is consumed to completion or not.
     *
     * @return a [CloseableIterator] of [Batch]es in transaction order.
     * @throws StatusException when this
     *  connection's database is no longer present on the server or for
     *  network or server errors
     */
    fun scanDatabaseLog(startAtRevision: Revision = 0uL): CloseableIterator<Batch>
}

/**
 * Databases represent a revision of the database identified by [name], i.e. a
 * point-in-time queryable basis for querying streams of events. Databases have value
 * semantics, can be compared for equality, etc.
 *
 * The clock state of a Database is its [revision], a monotonically increasing unsigned long
 * representing the count of events present in the database.
 *
 * After their parent client or connection is closed, all subsequent API method calls will
 * throw [ConnectionClosedException].
 */
interface Database {
    val name: DatabaseName
    val revision: Revision

    /**
     * Returns a [CloseableIterator] of [Event]s comprising
     * this stream as of this [Database]'s revision.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [Event]s comprising this stream, in transaction order.
     * @throws StatusException for network or server errors
     */
    fun fetchStream(streamName: StreamName): CloseableIterator<Event>

    /**
     * Returns a [CloseableIterator] of [Event]s comprising this subject stream as
     * of this [Database]'s revision, if both stream and events for the given subject
     * on that stream exist.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [Event]s comprising this stream, if any, in transaction order.
     */
    fun fetchSubjectStream(
        streamName: StreamName,
        subjectName: StreamSubject
    ): CloseableIterator<Event>


    /**
     * Returns a [CloseableIterator] of [Event]s comprising this subject across all streams as
     * of this [Database]'s revision.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [Event]s comprising this subject, if any, in transaction order.
     */
    fun fetchSubject(subjectName: StreamSubject): CloseableIterator<Event>

    /**
     * Returns a [CloseableIterator] of [Event]s having the given event type across all streams
     * as of this [Database]'s revision.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @return [CloseableIterator] of [Event]s having the given event type, if any, in transaction order.
     */
    fun fetchEventType(eventType: EventType): CloseableIterator<Event>

    /**
     * Returns a future bearing the [Event] having the given ID, if it exists.
     *
     * @return a [CompletableFuture] bearing the [Event]? if it exists w/in this database.
     */
    fun fetchEventById(streamName: StreamName, eventId: EventId): CompletableFuture<out Event?>
}


const val ITERATOR_READ_AHEAD_CACHE_SIZE = 100

@ThreadSafe
class FlowIterator<T>(
    private val flow: Flow<T>
): CloseableIterator<T> {
    private val asyncScope = CoroutineScope(Dispatchers.Default)
    private val blockingContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val queue = LinkedBlockingQueue<Option<T>>(ITERATOR_READ_AHEAD_CACHE_SIZE)
    private val nextItem: AtomicReference<Option<T>>

    init {
        asyncScope.launch {
            flow.collect {
                transfer(it.some())
            }
            transfer(None)
            close()
        }
        nextItem = AtomicReference(queue.take())
    }

    override fun hasNext(): Boolean =
        nextItem.get() != None

    override fun next(): T =
        if (hasNext()) {
            val currentItem = nextItem.get()
            nextItem.set(queue.take())
            when(currentItem) {
                None -> throw IndexOutOfBoundsException("Iterator bounds exceeded")
                is Some -> currentItem.value
            }
        }
        else
            throw IndexOutOfBoundsException("Iterator bounds exceeded")

    override fun close() {
        nextItem.set(None)
        asyncScope.cancel()
        blockingContext.close()
    }

    private suspend inline fun transfer(item: Option<T>) = withContext(blockingContext) {
        suspendCoroutine { continuation ->
            try {
                queue.put(item)
                continuation.resume(Unit)
            } catch (e: Exception) {
                continuation.resumeWithException(e)
            }
        }
    }
}

fun <T> Flow<T>.asIterator() =
    FlowIterator(this)