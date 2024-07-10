package com.evidentdb.client.base

import arrow.atomic.AtomicBoolean
import com.evidentdb.client.*
import com.evidentdb.client.java.asIterator
import com.evidentdb.client.transfer.toDomain
import com.evidentdb.client.transfer.toInstant
import com.evidentdb.client.transfer.toTransfer
import com.evidentdb.service.v1.*
import io.cloudevents.protobuf.toTransfer
import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CompletableFuture

class EvidentDbClientBase(
    private val channel: ManagedChannel,
    private val scope: CoroutineScope
): Shutdown {
    private val grpcClient = EvidentDbGrpcKt.EvidentDbCoroutineStub(channel)
    private val active = AtomicBoolean(true)

    val isActive: Boolean
        get() = active.get()

    fun createDatabase(name: DatabaseName) = runBlocking {
        createDatabaseAsync(name)
    }

    suspend fun createDatabaseAsync(name: DatabaseName) : Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            try {
                val result = grpcClient.createDatabase(
                    CreateDatabaseRequest.newBuilder()
                        .setName(name)
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

    fun transact(
        database: DatabaseName,
        batch: BatchProposal
    ): CompletableFuture<Batch> =
        scope.future { transactAsync(database, batch) }

    suspend fun transactAsync(
        database: DatabaseName,
        batch: BatchProposal
    ): Batch =
        if (!isActive)
            throw ConnectionClosedException(this)
        else {
            // Fail fast on empty batch, no need to round-trip
            if (batch.events.isEmpty())
                throw IllegalArgumentException("Batch cannot be empty")

            val result = grpcClient.transactBatch(
                TransactBatchRequest.newBuilder()
                    .setDatabase(database)
                    .addAllEvents(batch.events.map { it.toTransfer() })
                    .addAllConstraints(batch.constraints.map { it.toTransfer() })
                    .build()
            )
            result.batch.toDomain()
        }

    fun deleteDatabase(name: DatabaseName): Boolean =
        runBlocking { deleteDatabaseAsync(name) }

    suspend fun deleteDatabaseAsync(name: DatabaseName): Boolean =
        if (!isActive)
            throw ClientClosedException(this)
        else {
            try {
                val result = grpcClient.deleteDatabase(
                    DeleteDatabaseRequest.newBuilder()
                        .setName(name)
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

    fun fetchCatalog(): CloseableIterator<DatabaseName> =
        fetchCatalogAsync().asIterator()

    fun fetchCatalogAsync(): Flow<DatabaseName> =
        if (!isActive)
            throw ClientClosedException(this)
        else
            grpcClient
                .catalog(CatalogRequest.getDefaultInstance())
                .map { reply -> reply.databaseName }

    fun connect(database: DatabaseName): CloseableIterator<Database> =
        connectAsync(database).asIterator()

    fun connectAsync(database: DatabaseName): Flow<Database> =
        if (!isActive)
            throw ClientClosedException(this)
        else
            grpcClient
                .connect(ConnectRequest.newBuilder()
                    .setName(database)
                    .build())
                .map { reply -> reply.database.toDomain() }

    fun latestDatabase(database: DatabaseName): CompletableFuture<Database> =
        scope.future { latestDatabaseAsync(database) }

    suspend fun latestDatabaseAsync(database: DatabaseName): Database =
        if (!isActive) {
            throw ConnectionClosedException(this)
        } else {
            grpcClient.latestDatabase(
                LatestDatabaseRequest.newBuilder()
                    .setName(database)
                    .build()
            ).database.toDomain()
        }

    fun fetchDbAsOf(
        database: DatabaseName,
        revision: Revision
    ): CompletableFuture<Database> =
        scope.future {
            fetchDbAsOfAsync(database, revision)
        }

    /**
     * May block while awaiting database revision on server
     */
    suspend fun fetchDbAsOfAsync(
        database: DatabaseName,
        revision: Revision
    ): Database =
        if (!isActive) {
            throw ConnectionClosedException(this)
        } else {
            grpcClient.databaseAtRevision(
                DatabaseAtRevisionRequest.newBuilder()
                    .setName(database)
                    .setRevision(revision.toLong())
                    .build()
            ).database.toDomain()
        }

    fun fetchLog(database: DatabaseName): CloseableIterator<Batch> =
        fetchLogFlow(database).asIterator()

    fun fetchLogFlow(database: DatabaseName): Flow<Batch> =
        if (!isActive)
            throw ConnectionClosedException(this)
        else
            grpcClient.databaseLog(
                DatabaseLogRequest
                    .newBuilder()
                    .setName(database)
                    .build()
            ).map { batch ->
                val batchSummary = batch.batch
                val eventRevisions = (batchSummary.basis + 1).toULong()..batchSummary.revision.toULong()
                // fetch events by revision here
                val batchEvents = fetchEvents(eventRevisions.toList())
                Batch(
                    database,
                    batchSummary.basis.toULong(),
                    batchEvents.map { Event(it) }.toNonEmptyListOrNull()!!,
                    batchSummary.timestamp.toInstant(),
                )
            }

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
}