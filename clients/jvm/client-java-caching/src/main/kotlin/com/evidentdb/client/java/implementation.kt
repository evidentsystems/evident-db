package com.evidentdb.client.java

import com.evidentdb.client.*
import io.cloudevents.CloudEvent
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CompletableFuture
import com.evidentdb.client.kotlin.Connection as ConnectionKt
import com.evidentdb.client.kotlin.Database as DatabaseKt
import com.evidentdb.client.kotlin.EvidentDbCachingClient as EvidentDBKt

/**
 * @param channelBuilder The gRPC [io.grpc.ManagedChannelBuilder]
 *  for connecting to the EvidentDB server.
 * @constructor Main entry point for creating EvidentDB clients
 */
class EvidentDbCachingClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDb {
    // Init extension loading is handled by kotlinClient
    private val kotlinClient = EvidentDBKt(channelBuilder)

    override fun createDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.createDatabase(name) }

    override fun deleteDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.deleteDatabase(name) }

    override fun fetchCatalog(): CloseableIterator<DatabaseName> =
        kotlinClient.fetchCatalog().asIterator()

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
        override val databaseName: DatabaseName
            get() = kotlinConnection.databaseName
        private val connectionScope = CoroutineScope(Dispatchers.Default)

        override fun transact(
            events: List<CloudEvent>,
            constraints: List<BatchConstraint>,
        ): CompletableFuture<Batch> =
            connectionScope.future { kotlinConnection.transact(events, constraints) }

        override fun db(): Database = DatabaseImpl(kotlinConnection.db())

        override fun awaitDb(revision: Revision): CompletableFuture<Database> =
            connectionScope.future { DatabaseImpl(kotlinConnection.awaitDb(revision)) }

        override fun fetchLatestDb(): CompletableFuture<Database> =
            connectionScope.future { DatabaseImpl(kotlinConnection.fetchLatestDb()) }

        override fun scanDatabaseLog(startAtRevision: Revision): CloseableIterator<Batch> =
            kotlinConnection.scanDatabaseLog(startAtRevision).asIterator()

        override fun shutdown() { kotlinConnection.shutdown() }

        override fun shutdownNow() { kotlinConnection.shutdownNow() }
    }

    private class DatabaseImpl(private val kotlinDatabase: DatabaseKt): Database {
        override val name: DatabaseName
            get() = kotlinDatabase.name
        override val revision: Revision
            get() = kotlinDatabase.revision

        override fun fetchStream(streamName: StreamName): CloseableIterator<Event> =
            kotlinDatabase.fetchStream(streamName).asIterator()

        override fun fetchSubjectStream(
            streamName: StreamName,
            subjectName: StreamSubject
        ): CloseableIterator<Event> =
            kotlinDatabase
                .fetchSubjectStream(streamName, subjectName)
                .asIterator()

        override fun fetchSubject(subjectName: StreamSubject): CloseableIterator<Event> =
            kotlinDatabase.fetchSubject(subjectName).asIterator()

        override fun fetchEventType(eventType: EventType): CloseableIterator<Event> =
            kotlinDatabase.fetchEventType(eventType).asIterator()

        override fun fetchEventById(
            streamName: StreamName,
            eventId: EventId,
        ): CompletableFuture<out Event?> =
            runBlocking { future { kotlinDatabase.fetchEventById(streamName, eventId) } }

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
