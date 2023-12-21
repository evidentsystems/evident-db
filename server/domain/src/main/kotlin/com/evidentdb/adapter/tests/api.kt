package com.evidentdb.adapter.tests

import arrow.core.Either
import com.evidentdb.adapter.EvidentDbAdapter
import com.evidentdb.domain_model.Database
import com.evidentdb.domain_model.EvidentDbCommandError
import com.evidentdb.domain_model.ProposedEvent
import com.evidentdb.domain_model.QueryError
import io.cloudevents.core.builder.CloudEventBuilder
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import java.net.URI

interface AdapterTests {
    val databaseName: String
    val adapter: EvidentDbAdapter
    val creationResult: Either<EvidentDbCommandError, Database>

    fun `creating a database`() = runTest {
        Assertions.assertTrue(creationResult.isRight())
        Assertions.assertInstanceOf(Database::class.java, creationResult.getOrNull())

        // Ensure the updates are being sent upon connection
        val updates = mutableListOf<Either<QueryError, Database>>()
        val updatesJob = launch {
            adapter.connect(databaseName).toList(updates)
        }
        advanceUntilIdle()
        updatesJob.cancel()
        Assertions.assertEquals(1, updates.size)
        val update = updates.first()
        Assertions.assertTrue(update.isRight())
        Assertions.assertEquals(0uL, update.getOrNull()?.revision)

        // Ensure it shows up in catalog
        val catalog = adapter.catalog().toList()
        Assertions.assertEquals(
                listOf(databaseName),
                catalog.map { it.name.value }
        )
        Assertions.assertEquals(
                listOf(0uL),
                catalog.map { it.revision }
        )
    }

    fun `transacting batches`() = runTest {
        val revisionAfterBatch = 1uL
        // Ensure the updates are being sent upon connection
        val updates = mutableListOf<Either<QueryError, Database>>()
        val updatesJob = launch {
            adapter.connect(databaseName).toList(updates)
        }
        // Transact the Batch
        val eventStream = "my-stream"
        val eventId = "a nice id@foo.com"
        val eventSubject = "a nice subject"
        val eventType = "com.evidentdb.test-event"
        val events = listOf(
                ProposedEvent(
                        CloudEventBuilder.v1()
                                .withSource(URI(eventStream))
                                .withId(eventId)
                                .withSubject(eventSubject)
                                .withType(eventType)
                                .build()
                )
        )
        val result = adapter.transactBatch(databaseName, events, listOf())
        // Transaction should succeed
        Assertions.assertTrue(result.isRight())
        // New transaction should show up on log
        val log = adapter.databaseLog(databaseName, revisionAfterBatch).toList()
        Assertions.assertEquals(
                listOf(Pair(true, listOf(revisionAfterBatch))),
                log.map { Pair(it.isRight(), it.getOrNull()?.eventRevisions) }
        )
        // New transaction event should be indexed
        val eventById = adapter.eventById(
                databaseName,
                revisionAfterBatch,
                eventStream,
                eventId
        )
        Assertions.assertTrue(eventById.isRight())
        Assertions.assertEquals(
                Pair(eventStream, eventId),
                eventById.getOrNull()?.let {
                    Pair(it.stream.value, it.id.value)
                }
        )
        advanceUntilIdle()
        updatesJob.cancel()
        Assertions.assertEquals(1, updates.size)
        val update = updates.first()
        Assertions.assertTrue(update.isRight())
        Assertions.assertEquals(revisionAfterBatch, update.getOrNull()?.revision)
    }

    fun `querying various streams`() {

    }

    fun `deleting a database`() = runTest {
        val result = adapter.deleteDatabase(databaseName)
        Assertions.assertTrue(result.isRight())
        val catalog = adapter.catalog().toList()
        Assertions.assertEquals(
                listOf<Database>(),
                catalog
        )
    }
}