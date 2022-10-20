package com.evidentdb.client.java

import com.evidentdb.client.*
import io.cloudevents.CloudEvent
import com.evidentdb.client.kotlin.EvidentDB as EvidentDBKt
import com.evidentdb.client.kotlin.Connection as ConnectionKt
import com.evidentdb.client.kotlin.Database as DatabaseKt
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.future
import java.time.Instant
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class EvidentDB(channelBuilder: ManagedChannelBuilder<*>): Client {
    private val kotlinClient = EvidentDBKt(channelBuilder)

    override fun createDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.createDatabase(name) }

    override fun deleteDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.deleteDatabase(name) }

    override fun catalog(): CloseableIterator<DatabaseSummary> =
        kotlinClient.catalog().asIterator()

    override fun connectDatabase(name: DatabaseName): Connection =
        ConnectionImpl(kotlinClient.connectDatabase(name))

    override fun shutdown() {
        kotlinClient.shutdown()
    }

    override fun shutdownNow() {
        kotlinClient.shutdownNow()
    }

    private class ConnectionImpl(
        private val kotlinConnection: ConnectionKt
    ) : Connection {
        override val database: DatabaseName
            get() = kotlinConnection.database
        private val connectionScope = CoroutineScope(Dispatchers.Default)

        override fun transact(events: List<EventProposal>): CompletableFuture<Batch> =
            connectionScope.future {
                kotlinConnection.transact(events)
            }

        override fun db(): Database = DatabaseImpl(kotlinConnection.db())

        override fun db(revision: DatabaseRevision): CompletableFuture<Database> =
            connectionScope.future {
                DatabaseImpl(kotlinConnection.db(revision))
            }

        override fun sync(): CompletableFuture<Database> =
            connectionScope.future {
                DatabaseImpl(kotlinConnection.sync())
            }

        override fun log(): CloseableIterator<Batch> =
            kotlinConnection.log().asIterator()

        override fun shutdown() {
            kotlinConnection.shutdown()
        }

        override fun shutdownNow() {
            kotlinConnection.shutdownNow()
        }
    }

    private class DatabaseImpl(private val kotlinDatabase: DatabaseKt): Database {
        override val name: DatabaseName
            get() = kotlinDatabase.name
        override val created: Instant
            get() = kotlinDatabase.created
        override val streamRevisions: Map<StreamName, StreamRevision>
            get() = kotlinDatabase.streamRevisions
        override val revision: DatabaseRevision
            get() = kotlinDatabase.revision

        override fun stream(streamName: StreamName): CloseableIterator<CloudEvent> =
            kotlinDatabase.stream(streamName).asIterator()

        override fun subjectStream(
            streamName: StreamName,
            subjectName: StreamSubject
        ): CloseableIterator<CloudEvent> =
            kotlinDatabase
                .subjectStream(streamName, subjectName)
                .asIterator()

        override fun event(eventId: EventId): CompletableFuture<CloudEvent?> =
            runBlocking { future { kotlinDatabase.event(eventId) } }

        // Use as Data Class
        operator fun component1() = name
        operator fun component2() = created
        operator fun component3() = streamRevisions

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as DatabaseImpl

            if (name != other.name) return false
            if (created != other.created) return false
            if (streamRevisions != other.streamRevisions) return false

            return true
        }

        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + created.hashCode()
            result = 31 * result + streamRevisions.hashCode()
            return result
        }

        override fun toString(): String {
            return "Database(name='$name', created=$created, streamRevisions=$streamRevisions, revision=$revision)"
        }
    }
}

private fun <T> Flow<T>.asIterator() =
    FlowIterator(this)

internal class FlowIterator<T>(
    private val flow: Flow<T>
): CloseableIterator<T> {
    private val asyncScope = CoroutineScope(Dispatchers.Default)
    private val queue = SynchronousQueue<T>()
    private val closed = AtomicBoolean(false)

    init {
        asyncScope.launch {
            try {
                flow.collect {
                    transfer(it)
                }
            } finally {
                closed.set(true)
            }
        }
    }

    override fun hasNext(): Boolean =
        !closed.get()

    override fun next(): T =
        queue.take()

    override fun close() {
        closed.set(true)
        asyncScope.cancel()
    }

    private suspend inline fun transfer(item: T) = withContext(Dispatchers.IO) {
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
