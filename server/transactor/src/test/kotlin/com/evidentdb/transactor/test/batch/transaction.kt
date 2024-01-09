package com.evidentdb.transactor.test.batch

import com.evidentdb.application.StreamKey
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import com.evidentdb.test.buildTestEvent
import com.evidentdb.transactor.TransactorTopology
import com.evidentdb.transactor.test.TopologyTestDriverCommandService
import com.evidentdb.transactor.test.driver
import io.cloudevents.CloudEvent
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TransactionTests {
    @Test
    fun `fast reject transaction with no events`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = "foo"

            val result = service.transactBatch(databaseName, listOf())
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is EmptyBatch) }
        }

    @Test
    fun `reject transaction when database is not found`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = "foo"

            val result = service.transactBatch(databaseName, listOf(
                ProposedEvent(buildTestEvent("event.invalidated.stream"), "foo"),
            ))
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFound) }
        }

    @Test
    fun `reject transaction due to invalid events`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val result = service.transactBatch(databaseName, listOf(
                ProposedEvent(buildTestEvent("event.invalidated.stream"), ""),
                ProposedEvent(buildTestEvent(""), "event.invalidated.type"),
            ))
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                Assertions.assertTrue(it is InvalidEvents)
                val err = it as InvalidEvents
                Assertions.assertTrue(err.invalidEvents[0].errors[0] is InvalidStreamName)
                Assertions.assertTrue(err.invalidEvents[1].errors[0] is InvalidEventType)
            }
        }

    @Test
    fun `reject transaction due to stream state constraints`(): Unit =
        runBlocking {
            val driver = driver()
            val streamStore = driver.getKeyValueStore<StreamKey, Stream>(TransactorTopology.STREAM_STORE)
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val existingStreamName = "my-stream"
            val initBatch = listOf(
                ProposedEvent(
                    buildTestEvent("event.stream.initialize"),
                    existingStreamName,
                    StreamState.NoStream
                )
            )

            val initResult = service.transactBatch(databaseName, initBatch)
            Assertions.assertTrue(initResult.isRight())

            val batch = listOf(
                ProposedEvent(
                    buildTestEvent("event.stream.does-not-exist-but-should"),
                    "a-new-stream",
                    StreamState.StreamExists
                ),
                ProposedEvent(
                    buildTestEvent("event.stream.exists-but-should-not"),
                    existingStreamName,
                    StreamState.NoStream
                ),
                ProposedEvent(
                    buildTestEvent("event.stream.at-wrong-revision"),
                    existingStreamName,
                    StreamState.AtRevision(100)
                ),
                ProposedEvent(
                    buildTestEvent("event.stream.no-error"),
                    "any-stream"
                ),
            )
            val result = service.transactBatch(databaseName, batch)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                Assertions.assertTrue(it is BatchConstraintViolations)
                val err = it as BatchConstraintViolations
                Assertions.assertEquals(err.violations.size, 3)
                Assertions.assertEquals(err.violations[0].event.event.type, batch[0].event.type)
                Assertions.assertEquals(err.violations[1].event.event.type, batch[1].event.type)
                Assertions.assertEquals(err.violations[2].event.event.type, batch[2].event.type)
            }


            Assertions.assertNull(
                streamStore.get(
                    buildStreamKey(
                        DatabaseName.build(databaseName),
                        "a-new-stream")
                )
            )
            Assertions.assertNull(
                streamStore.get(
                    buildStreamKey(
                        DatabaseName.build(databaseName),
                        "any-stream"
                    )
                )
            )
        }

    @Test
    fun `accept transaction with various stream state constraints`(): Unit =
        runBlocking {
            val driver = driver()
            val batchStore = driver.getKeyValueStore<BatchId, DatabaseLogKey>(TransactorTopology.BATCH_STORE)
            val streamStore = driver.getKeyValueStore<StreamKey, Stream>(TransactorTopology.STREAM_STORE)
            val eventStore = driver.getKeyValueStore<EventRevision, CloudEvent>(TransactorTopology.EVENT_STORE)
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = DatabaseName.build("foo")
            service.createDatabase(databaseName.value)

            val existingStreamName = "my-stream"
            val initBatch = listOf(
                ProposedEvent(
                    buildTestEvent("event.stream.initialize"),
                    existingStreamName,
                    StreamState.NoStream
                )
            )

            val initResult = service.transactBatch(databaseName.value, initBatch)
            Assertions.assertTrue(initResult.isRight())

            val batch = listOf(
                ProposedEvent(
                    buildTestEvent("event.stream.no-stream"),
                    "a-new-stream",
                    StreamState.NoStream
                ),
                ProposedEvent(
                    buildTestEvent("event.stream.exists"),
                    existingStreamName,
                    StreamState.StreamExists
                ),
                ProposedEvent(
                    buildTestEvent("event.stream.revision"),
                    existingStreamName,
                    StreamState.AtRevision(1)
                ),
                ProposedEvent(
                    buildTestEvent("event.stream.no-error"),
                    "any-stream"
                ),
            )
            val result = service.transactBatch(databaseName.value, batch)
            Assertions.assertTrue(result.isRight())
            result.map {
                Assertions.assertEquals(it.data.batch.events.size, 4)
                Assertions.assertNotNull(
                    batchStore.get(
                        it.data.batch.id
                    )
                )
                for (event in it.data.batch.events) {
                    Assertions.assertEquals(
                        event.event,
                        eventStore.get(event.id)
                    )
                }
            }
            Assertions.assertNotNull(
                streamStore.get(
                    buildStreamKey(
                        databaseName,
                        "a-new-stream"
                    )
                )
            )
            Assertions.assertNotNull(
                streamStore.get(
                    buildStreamKey(
                        databaseName,
                        "any-stream"
                    )
                )
            )
            Assertions.assertNotNull(
                streamStore.get(
                    buildStreamKey(
                        databaseName,
                        existingStreamName
                    )
                )
            )
        }
}