package com.evidentdb.client.java

import com.evidentdb.client.*
import io.cloudevents.CloudEvent
import com.evidentdb.client.kotlin.GrpcClientKt as EvidentDBKt
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import java.time.Instant
import java.util.concurrent.*

class GrpcClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDb {
    private val kotlinClient = EvidentDBKt(channelBuilder)

    override fun createDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.createDatabaseAsync(name) }

    override fun deleteDatabase(name: DatabaseName): Boolean =
        runBlocking { kotlinClient.deleteDatabaseAsync(name) }

    override fun fetchCatalog(): CloseableIterator<DatabaseSummary> =
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

        override fun transact(events: List<EventProposal>): CompletableFuture<Batch> =
            connectionScope.future {
                kotlinConnection.transactAsync(events)
            }

        override fun db(): Database = DatabaseImpl(kotlinConnection.db())

        override fun fetchDbAsOf(revision: DatabaseRevision): CompletableFuture<Database> =
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
        override val topic: TopicName
            get() = kotlinDatabase.topic
        override val created: Instant
            get() = kotlinDatabase.created
        override val streamRevisions: Map<StreamName, StreamRevision>
            get() = kotlinDatabase.streamRevisions
        override val revision: DatabaseRevision
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

        override fun fetchEvent(eventId: EventId): CompletableFuture<CloudEvent?> =
            runBlocking { future { kotlinDatabase.fetchEventAsync(eventId) } }

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
            return "Database(name='$name', topic=$topic, created=$created, streamRevisions=$streamRevisions, revision=$revision)"
        }
    }
}
