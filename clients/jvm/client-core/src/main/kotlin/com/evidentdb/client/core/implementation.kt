package com.evidentdb.client.core

import arrow.atomic.AtomicBoolean
import com.evidentdb.client.*
import com.evidentdb.client.transfer.toDomain
import com.evidentdb.client.transfer.toTransfer
import com.evidentdb.v1.proto.service.*
import io.cloudevents.CloudEvent
import io.cloudevents.protobuf.toDomain
import io.cloudevents.protobuf.toTransfer
import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.*

class EvidentDb(
    private val channel: ManagedChannel,
): Shutdown {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)
    private val active = AtomicBoolean(true)

    // Lifecycle

    val isActive: Boolean
        get() = active.get()

    override fun shutdown() {
        if (isActive) {
            active.set(false)
            channel.shutdown()
        }
    }

    override fun shutdownNow() {
        if (isActive) {
            active.set(false)
            channel.shutdownNow()
        }
    }

    // Client

    suspend fun createDatabase(
        name: DatabaseName
    ) : Boolean = whenActiveSuspend {
        try {
            val result = grpcClient.createDatabase(
                CreateDatabaseRequest.newBuilder()
                    .setDatabaseName(name)
                    .build()
            )
            result.hasDatabase()
        } catch (e: StatusException) {
            if (e.status.code == Status.Code.ALREADY_EXISTS) {
                false
            } else {
                throw e
            }
        }
    }

    suspend fun transact(
        database: DatabaseName,
        events: List<CloudEvent>,
        constraints: List<BatchConstraint>,
    ): Batch = whenActiveSuspend {
        // Fail fast on empty batch, no need to round-trip
        if (events.isEmpty())
            throw IllegalArgumentException("Batch cannot be empty")

        val result = grpcClient.transactBatch(
            TransactBatchRequest.newBuilder()
                .setDatabaseName(database)
                .addAllEvents(events.map { it.toTransfer() })
                .addAllConstraints(constraints.map { it.toTransfer() })
                .build()
        )
        result.batch.toDomain()
    }

    suspend fun deleteDatabase(
        name: DatabaseName
    ): Boolean = whenActiveSuspend {
        try {
            val result = grpcClient.deleteDatabase(
                DeleteDatabaseRequest.newBuilder()
                    .setDatabaseName(name)
                    .build()
            )
            result.hasDatabase()
        } catch (e: StatusException) {
            if (e.status.code == Status.Code.NOT_FOUND) {
                false
            } else {
                throw e
            }
        }
    }

    fun fetchCatalog(): Flow<DatabaseName> = whenActive {
        grpcClient
            .fetchCatalog(CatalogRequest.getDefaultInstance())
            .map { reply -> reply.databaseName }
    }

    suspend fun fetchLatestDatabase(
        database: DatabaseName
    ): Database = whenActiveSuspend {
        grpcClient.fetchLatestDatabase(
            LatestDatabaseRequest.newBuilder()
                .setDatabaseName(database)
                .build()
        ).database.toDomain()
    }

    /**
     * May suspend indefinitely while awaiting database revision on server
     */
    suspend fun awaitDatabase(
        database: DatabaseName,
        atLeastRevision: Revision
    ): Database = whenActiveSuspend {
        grpcClient.awaitDatabase(
            AwaitDatabaseRequest.newBuilder()
                .setDatabaseName(database)
                .setAtLeastRevision(atLeastRevision.toLong())
                .build()
        ).database.toDomain()
    }

    fun subscribeDatabaseUpdates(
        database: DatabaseName
    ): Flow<Database> = whenActive {
        grpcClient.subscribeDatabaseUpdates(
            DatabaseUpdatesSubscriptionRequest.newBuilder()
                .setDatabaseName(database)
                .build()
        ).map { it.database.toDomain() }
    }

    fun scanDatabaseLog(
        database: DatabaseName,
        startAtRevision: Revision = 0uL,
    ): Flow<BatchSummary> = whenActive {
        grpcClient.scanDatabaseLog(
            LogScanRequest
                .newBuilder()
                .setIncludeEventDetail(false)
                .setStartAtRevision(startAtRevision.toLong())
                .setDatabaseName(database)
                .build()
        ).map { it.summary.toDomain() }
    }

    fun scanDatabaseLogDetail(
        database: DatabaseName,
        startAtRevision: Revision = 0uL,
    ): Flow<Batch> = whenActive {
        grpcClient.scanDatabaseLog(
            LogScanRequest
                .newBuilder()
                .setIncludeEventDetail(true)
                .setStartAtRevision(startAtRevision.toLong())
                .setDatabaseName(database)
                .build()
        ).map { it.detail.toDomain() }
    }

    fun fetchEventRevisionsByStream(
        databaseNameStr: String,
        revision: Revision,
        streamNameStr: String,
    ): Flow<Revision> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(false)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setStream(
                    EventIndexRequest.StreamQuery.newBuilder().setStream(streamNameStr)
                ).build()
        ).map { it.revision.toULong() }
    }


    fun fetchEventsByStream(
        databaseNameStr: String,
        revision: Revision,
        streamNameStr: String,
    ): Flow<Event> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(true)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setStream(
                    EventIndexRequest.StreamQuery.newBuilder().setStream(streamNameStr)
                ).build()
        ).map { Event(it.detail.toDomain()) }
    }

    fun fetchEventRevisionsBySubjectAndStream(
        databaseNameStr: String,
        revision: Revision,
        streamNameStr: String,
        subjectStr: String,
    ): Flow<Revision> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(false)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setSubjectStream(
                    EventIndexRequest.SubjectStreamQuery.newBuilder()
                        .setStream(streamNameStr)
                        .setSubject(subjectStr)
                ).build()
        ).map { it.revision.toULong() }
    }

    fun fetchEventsBySubjectAndStream(
        databaseNameStr: String,
        revision: Revision,
        streamNameStr: String,
        subjectStr: String,
    ): Flow<Event> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(true)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setSubjectStream(
                    EventIndexRequest.SubjectStreamQuery.newBuilder()
                        .setStream(streamNameStr)
                        .setSubject(subjectStr)
                ).build()
        ).map { Event(it.detail.toDomain()) }
    }

    fun fetchEventRevisionsBySubject(
        databaseNameStr: String,
        revision: Revision,
        subjectStr: String,
    ): Flow<Revision> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(false)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setSubject(
                    EventIndexRequest.SubjectQuery.newBuilder().setSubject(subjectStr)
                ).build()
        ).map { it.revision.toULong() }
    }

    fun fetchEventsBySubject(
        databaseNameStr: String,
        revision: Revision,
        subjectStr: String,
    ): Flow<Event> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(true)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setSubject(
                    EventIndexRequest.SubjectQuery.newBuilder().setSubject(subjectStr)
                ).build()
        ).map { Event(it.detail.toDomain()) }
    }

    fun fetchEventRevisionsByType(
        databaseNameStr: String,
        revision: Revision,
        eventTypeStr: String,
    ): Flow<Revision> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(false)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setEventType(
                    EventIndexRequest.EventTypeQuery.newBuilder().setEventType(eventTypeStr)
                ).build()
        ).map { it.revision.toULong() }
    }

    fun fetchEventsByType(
        databaseNameStr: String,
        revision: Revision,
        eventTypeStr: String,
    ): Flow<Event> = whenActive {
        grpcClient.queryEventIndex(
            EventIndexRequest.newBuilder()
                .setIncludeEventDetail(true)
                .setDatabaseName(databaseNameStr)
                .setRevision(revision.toLong())
                .setEventType(
                    EventIndexRequest.EventTypeQuery.newBuilder().setEventType(eventTypeStr)
                ).build()
        ).map { Event(it.detail.toDomain()) }
    }

    suspend fun fetchEventById(
        databaseNameStr: String,
        revision: Revision,
        streamNameStr: String,
        eventIdStr: String,
    ): Event? = whenActiveSuspend {
        try {
            grpcClient.queryEventIndex(
                EventIndexRequest.newBuilder()
                    .setIncludeEventDetail(true)
                    .setDatabaseName(databaseNameStr)
                    .setRevision(revision.toLong())
                    .setEventById(
                        EventIndexRequest.EventByIdQuery.newBuilder()
                            .setStream(streamNameStr)
                            .setEventId(eventIdStr)
                    ).build()
            ).map { Event(it.detail.toDomain()) }.first()
        } catch (t: StatusException) {
            if (t.status == Status.NOT_FOUND) {
                null
            } else {
                throw t
            }
        }
    }

    fun fetchEventsByRevisions(
        databaseNameStr: String,
        revisions: List<Revision>,
    ): Flow<Event> = whenActive {
        grpcClient.fetchEventsByRevisions(
            EventsByRevisionsRequest.newBuilder()
                .setDatabaseName(databaseNameStr)
                .addAllEventRevisions(revisions.map { it.toLong() })
                .build()
        ).map { Event(it.event.toDomain()) }
    }

    private fun <A> whenActive(block: () -> A) = if (isActive) {
        block()
    } else {
        throw ClientClosedException(this)
    }

    private suspend fun <A> whenActiveSuspend(block: suspend () -> A) = if (isActive) {
        block()
    } else {
        throw ClientClosedException(this)
    }
}