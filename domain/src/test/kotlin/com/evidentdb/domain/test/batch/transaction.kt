package com.evidentdb.domain.test.batch

import com.evidentdb.domain.*
import com.evidentdb.domain.test.InMemoryService
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TransactionTests {
    @Test
    fun `fast reject transaction when database is not found`(): Unit =
        runBlocking {
            val service = InMemoryService.empty()
            val result = service.transactBatch("foo", listOf())
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
        }

    @Test
    fun `reject transaction with no events`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val database = Database(DatabaseId.randomUUID(), databaseName)
            val service = InMemoryService(listOf(database), listOf())
            val result = service.transactBatch(databaseName, listOf())
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is NoEventsProvided) }
        }

    @Test
    fun `reject transaction due to invalid events`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val database = Database(DatabaseId.randomUUID(), databaseName)
            val service = InMemoryService(listOf(database), listOf())
            val result = service.transactBatch(databaseName, listOf(
                UnvalidatedProposedEvent("event.invalidated.stream", ""),
                UnvalidatedProposedEvent("", "event.invalidated.type"),
                UnvalidatedProposedEvent(
                    "event.invalidated.attribute",
                    "errors",
                    StreamState.Any,
                    null,
                    mapOf(Pair("", EventAttributeValue.StringValue("value to an invalid attribute key")))
                ),
            ))
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                Assertions.assertTrue(it is InvalidEventsError)
                val err = it as InvalidEventsError
                Assertions.assertTrue(err.errors[0].errors[0] is InvalidStreamName)
                Assertions.assertTrue(err.errors[1].errors[0] is InvalidEventType)
                Assertions.assertTrue(err.errors[2].errors[0] is InvalidEventAttribute)
            }
        }

    @Test
    fun `reject transaction due to stream state constraints`(): Unit =
        runBlocking {
            val database = Database(DatabaseId.randomUUID(), "foo")
            val existingStream = Stream.create(database.id, "my-stream")
            val service = InMemoryService(listOf(database), listOf(existingStream))
            val batch = listOf(
                UnvalidatedProposedEvent(
                    "event.stream.does-not-exist-but-should",
                    "a-new-stream",
                    StreamState.StreamExists
                ),
                UnvalidatedProposedEvent(
                    "event.stream.exists-but-should-not",
                    existingStream.name,
                    StreamState.NoStream
                ),
                UnvalidatedProposedEvent(
                    "event.stream.at-wrong-revision",
                    existingStream.name,
                    StreamState.AtRevision(100)
                ),
                UnvalidatedProposedEvent(
                    "event.stream.no-error",
                    "any-stream"
                ),
            )
            val result = service.transactBatch(database.name, batch)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                Assertions.assertTrue(it is StreamStateConflictsError)
                val err = it as StreamStateConflictsError
                Assertions.assertEquals(err.errors.size, 3)
                Assertions.assertEquals(err.errors[0].event.type, batch[0].type)
                Assertions.assertEquals(err.errors[1].event.type, batch[1].type)
                Assertions.assertEquals(err.errors[2].event.type, batch[2].type)
            }
        }

    @Test
    fun `accept transaction with various stream state constraints`(): Unit =
        runBlocking {
            val database = Database(DatabaseId.randomUUID(), "foo")
            val existingStream = Stream.create(database.id, "my-stream")
            val service = InMemoryService(listOf(database), listOf(existingStream))
            val batch = listOf(
                UnvalidatedProposedEvent(
                    "event.stream.no-stream",
                    "a-new-stream",
                    StreamState.NoStream
                ),
                UnvalidatedProposedEvent(
                    "event.stream.exists",
                    existingStream.name,
                    StreamState.StreamExists
                ),
                UnvalidatedProposedEvent(
                    "event.stream.revision",
                    existingStream.name,
                    StreamState.AtRevision(0)
                ),
                UnvalidatedProposedEvent(
                    "event.stream.no-error",
                    "any-stream"
                ),
            )
            val result = service.transactBatch(database.name, batch)
            Assertions.assertTrue(result.isRight())
            result.map {
                Assertions.assertEquals(it.data.events.size, 4)
            }
        }
}