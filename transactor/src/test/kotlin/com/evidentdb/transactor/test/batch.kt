package com.evidentdb.transactor.test

import com.evidentdb.domain.*
import com.evidentdb.domain.v1.proto.Database
import com.evidentdb.transactor.Topology
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class BatchTests {
    @Test
    fun `accept transaction with various stream state constraints`(): Unit =
        runBlocking {
            val driver = driver()
            val databaseStore = driver.getKeyValueStore<DatabaseId, Database>(Topology.DATABASE_STORE)
            val databaseNameStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(Topology.DATABASE_NAME_LOOKUP)
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val existingStreamName = "my-stream"
            val initBatch = listOf(
                UnvalidatedProposedEvent(
                    "event.stream.no-stream",
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
                    StreamState.AtRevision(0)
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
        }
}