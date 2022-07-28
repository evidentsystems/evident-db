package com.evidentdb.transactor.test.batch

import com.evidentdb.domain.*
import com.evidentdb.dto.v1.proto.Database
import com.evidentdb.transactor.TransactorTopology
import com.evidentdb.transactor.test.TopologyTestDriverService
import com.evidentdb.transactor.test.driver
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TransactionTests {
    @Test
    fun `fast reject transaction when database is not found`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"

            val result = service.transactBatch(databaseName, listOf())
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
        }

    @Test
    fun `reject transaction with no events`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val result = service.transactBatch(databaseName, listOf())
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is NoEventsProvidedError) }
        }

    @Test
    fun `reject transaction due to invalid events`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

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
                Assertions.assertTrue(err.invalidEvents[0].errors[0] is InvalidStreamName)
                Assertions.assertTrue(err.invalidEvents[1].errors[0] is InvalidEventType)
                Assertions.assertTrue(err.invalidEvents[2].errors[0] is InvalidEventAttribute)
            }
        }

    @Test
    fun `reject transaction due to stream state constraints`(): Unit =
        runBlocking {
            val driver = driver()
            val batchStore = driver.getKeyValueStore<DatabaseId, Database>(TransactorTopology.BATCH_STORE)
            val streamStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(TransactorTopology.STREAM_STORE)
            val eventStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(TransactorTopology.EVENT_STORE)
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val existingStreamName = "my-stream"
            val initBatch = listOf(
                UnvalidatedProposedEvent(
                    "event.stream.initialize",
                    existingStreamName,
                    StreamState.NoStream
                )
            )

            val initResult = service.transactBatch(databaseName, initBatch)
            Assertions.assertTrue(initResult.isRight())

            val batch = listOf(
                UnvalidatedProposedEvent(
                    "event.stream.does-not-exist-but-should",
                    "a-new-stream",
                    StreamState.StreamExists
                ),
                UnvalidatedProposedEvent(
                    "event.stream.exists-but-should-not",
                    existingStreamName,
                    StreamState.NoStream
                ),
                UnvalidatedProposedEvent(
                    "event.stream.at-wrong-revision",
                    existingStreamName,
                    StreamState.AtRevision(100)
                ),
                UnvalidatedProposedEvent(
                    "event.stream.no-error",
                    "any-stream"
                ),
            )
            val result = service.transactBatch(databaseName, batch)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                Assertions.assertTrue(it is StreamStateConflictsError)
                val err = it as StreamStateConflictsError
                Assertions.assertEquals(err.conflicts.size, 3)
                Assertions.assertEquals(err.conflicts[0].event.type, batch[0].type)
                Assertions.assertEquals(err.conflicts[1].event.type, batch[1].type)
                Assertions.assertEquals(err.conflicts[2].event.type, batch[2].type)
            }
            TODO("assert events are not present in storage")
        }

    @Test
    fun `accept transaction with various stream state constraints`(): Unit =
        runBlocking {
            val driver = driver()
            val batchStore = driver.getKeyValueStore<DatabaseId, Database>(TransactorTopology.BATCH_STORE)
            val streamStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(TransactorTopology.STREAM_STORE)
            val eventStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(TransactorTopology.EVENT_STORE)
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val existingStreamName = "my-stream"
            val initBatch = listOf(
                UnvalidatedProposedEvent(
                    "event.stream.initialize",
                    existingStreamName,
                    StreamState.NoStream
                )
            )

            val initResult = service.transactBatch(databaseName, initBatch)
            Assertions.assertTrue(initResult.isRight())

            val batch = listOf(
                UnvalidatedProposedEvent(
                    "event.stream.no-stream",
                    "a-new-stream",
                    StreamState.NoStream
                ),
                UnvalidatedProposedEvent(
                    "event.stream.exists",
                    existingStreamName,
                    StreamState.StreamExists
                ),
                UnvalidatedProposedEvent(
                    "event.stream.revision",
                    existingStreamName,
                    StreamState.AtRevision(1)
                ),
                UnvalidatedProposedEvent(
                    "event.stream.no-error",
                    "any-stream"
                ),
            )
            val result = service.transactBatch(databaseName, batch)
            Assertions.assertTrue(result.isRight())
            result.map {
                Assertions.assertEquals(it.data.events.size, 4)
            }
            TODO("assert entities present in storage")
        }
}