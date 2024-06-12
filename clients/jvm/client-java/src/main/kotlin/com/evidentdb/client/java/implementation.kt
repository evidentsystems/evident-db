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
import com.evidentdb.client.kotlin.KotlinSimpleClient as EvidentDBKt

fun EvidentDb.Companion.javaClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDb =
    JavaSimpleClient(channelBuilder)

class JavaSimpleClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDb {
    private val kotlinClient = EvidentDBKt(channelBuilder)

    override fun createDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.createDatabaseAsync(name) }

    override fun deleteDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.deleteDatabaseAsync(name) }

    override fun fetchCatalog(): CloseableIterator<DatabaseName> =
        kotlinClient.fetchCatalogAsync().asIterator()

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

        override fun transact(batch: BatchProposal): CompletableFuture<Batch> =
            connectionScope.future {
                kotlinConnection.transactAsync(batch)
            }

        override fun db(): Database = DatabaseImpl(kotlinConnection.db())

        override fun fetchDbAsOf(revision: Revision): CompletableFuture<Database> =
            connectionScope.future {
                DatabaseImpl(kotlinConnection.fetchDbAsOfAsync(revision))
            }

        override fun fetchLatestDb(): CompletableFuture<Database> =
            connectionScope.future {
                DatabaseImpl(kotlinConnection.fetchLatestDbAsync())
            }

        override fun fetchLog(): CloseableIterator<Batch> =
            kotlinConnection.fetchLogFlow().asIterator()

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
        override val revision: Revision
            get() = kotlinDatabase.revision

        override fun fetchStream(streamName: StreamName): CloseableIterator<CloudEvent> =
            kotlinDatabase.fetchStreamAsync(streamName).asIterator()

        override fun fetchSubjectStream(
            streamName: StreamName,
            subjectName: StreamSubject
        ): CloseableIterator<CloudEvent> =
            kotlinDatabase
                .fetchSubjectStreamAsync(streamName, subjectName)
                .asIterator()

        override fun fetchSubject(subjectName: StreamSubject): CloseableIterator<CloudEvent> =
            kotlinDatabase.fetchSubjectAsync(subjectName).asIterator()

        override fun fetchEventType(eventType: EventType): CloseableIterator<CloudEvent> =
            kotlinDatabase.fetchEventTypeAsync(eventType).asIterator()

    override fun fetchEventById(eventId: EventId): CompletableFuture<out CloudEvent?> =
            runBlocking { future { kotlinDatabase.fetchEventByIdAsync(eventId) } }

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
