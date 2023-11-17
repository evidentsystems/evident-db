package com.evidentdb.domain_model.test.batch

import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import com.evidentdb.domain_model.test.InMemoryCommandService
import com.evidentdb.domain_model.test.buildTestDatabase
import com.evidentdb.test.buildTestEvent
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TransactionTests {
    @Test
    fun `fast reject transaction with no events`(): Unit =
        runBlocking {
            val service = InMemoryCommandService.empty()
            val result = service.transactBatch("foo", listOf())
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is NoEventsProvidedError) }
        }

    @Test
    fun `reject transaction when database is not found`(): Unit =
        runBlocking {
            val service = InMemoryCommandService.empty()
            val result = service.transactBatch("foo", listOf(
                UnvalidatedProposedEvent(buildTestEvent("event.valid"), "foo"),
            ))
            println(result)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
        }

    @Test
    fun `reject transaction due to invalid events`(): Unit =
        runBlocking {
            val databaseName = DatabaseName.build("foo")
            val database = buildTestDatabase(databaseName)
            val service = InMemoryCommandService(listOf(database), listOf(), listOf())
            val result = service.transactBatch(databaseName.value, listOf(
                UnvalidatedProposedEvent(buildTestEvent("event.invalidated.stream"), ""),
                UnvalidatedProposedEvent(buildTestEvent(""), "event.invalidated.type"),
            ))
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                Assertions.assertTrue(it is InvalidEventsError)
                val err = it as InvalidEventsError
                Assertions.assertTrue(err.invalidEvents[0].errors[0] is InvalidStreamName)
                Assertions.assertTrue(err.invalidEvents[1].errors[0] is InvalidEventType)
            }
        }

    @Test
    fun `reject transaction due to stream state constraints`(): Unit =
        runBlocking {
            val database = buildTestDatabase(DatabaseName.build("foo"))
            val existingStream = StreamSummary.create(database.name, "my-stream")
            val service = InMemoryCommandService(listOf(database), listOf(existingStream), listOf())
            val batch = listOf(
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.does-not-exist-but-should"),
                    "a-new-stream",
                    StreamState.StreamExists
                ),
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.exists-but-should-not"),
                    existingStream.name,
                    StreamState.NoStream
                ),
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.at-wrong-revision"),
                    existingStream.name,
                    StreamState.AtRevision(100)
                ),
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.no-error"),
                    "any-stream"
                ),
            )
            val result = service.transactBatch(database.name.value, batch)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                Assertions.assertTrue(it is StreamStateConflictsError)
                val err = it as StreamStateConflictsError
                Assertions.assertEquals(err.conflicts.size, 3)
                Assertions.assertEquals(err.conflicts[0].event.event.type, batch[0].event.type)
                Assertions.assertEquals(err.conflicts[1].event.event.type, batch[1].event.type)
                Assertions.assertEquals(err.conflicts[2].event.event.type, batch[2].event.type)
            }
        }

    @Test
    fun `accept transaction with various stream state constraints`(): Unit =
        runBlocking {
            val database = buildTestDatabase(DatabaseName.build("foo"))
            val existingStream = StreamSummary.create(database.name, "my-stream")
            val service = InMemoryCommandService(listOf(database), listOf(existingStream), listOf())
            val batch = listOf(
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.no-stream"),
                    "a-new-stream",
                    StreamState.NoStream
                ),
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.exists"),
                    existingStream.name,
                    StreamState.StreamExists
                ),
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.revision"),
                    existingStream.name,
                    StreamState.AtRevision(0)
                ),
                UnvalidatedProposedEvent(
                    buildTestEvent("event.stream.no-error"),
                    "any-stream"
                ),
            )
            val result = service.transactBatch(database.name.value, batch)
            Assertions.assertTrue(result.isRight())
            result.map {
                Assertions.assertEquals(it.data.batch.events.size, 4)
            }
        }
}