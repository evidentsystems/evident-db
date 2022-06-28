package com.evidentdb.transactor.test

import com.evidentdb.domain.*
import com.evidentdb.transactor.Topology
import kotlinx.coroutines.runBlocking
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.*

class TopologyTests {
    @Test
    fun `topology creates a database`(): Unit =
        runBlocking {
            val driver = driver()
            val databaseStore = driver.getKeyValueStore<DatabaseId, Database>(Topology.DATABASE_STORE)
            val databaseNameStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(Topology.DATABASE_NAME_LOOKUP)
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"

            val event = service.createDatabase(databaseName)

            Assertions.assertTrue(event.isRight())
            event.map {
                Assertions.assertEquals(
                    databaseStore.get(it.databaseId),
                    it.data.database
                )

                Assertions.assertEquals(
                    it.databaseId,
                    databaseNameStore.get(databaseName)
                )

                Assertions.assertTrue(
                    driver.producedTopicNames().contains("databases")
                )
                Assertions.assertTrue(
                    driver.producedTopicNames().contains("internal-events")
                )
                Assertions.assertTrue(
                    driver.producedTopicNames().contains("database-names")
                )
            }
        }

    @Test
    fun `topology renames a database`(): Unit =
        runBlocking {
            val driver = driver()
            val databaseStore = driver.getKeyValueStore<DatabaseId, Database>(Topology.DATABASE_STORE)
            val databaseNameStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(Topology.DATABASE_NAME_LOOKUP)
            val service = TopologyTestDriverService(driver)
            val oldName = "foo"
            val newName = "bar"

            service.createDatabase(oldName)

            val event = service.renameDatabase(oldName, newName)

            event.map {
                val database = databaseStore.get(it.databaseId)
                Assertions.assertEquals(
                    Database(it.databaseId, newName),
                    database
                )

                Assertions.assertEquals(it.databaseId, databaseNameStore.get(newName))

                Assertions.assertTrue(
                    driver.producedTopicNames().contains("databases")
                )
                Assertions.assertTrue(
                    driver.producedTopicNames().contains("internal-events")
                )
                Assertions.assertTrue(
                    driver.producedTopicNames().contains("database-names")
                )
            }
        }

    @Test
    fun `topology deletes a database`(): Unit =
        runBlocking {
            val driver = driver()
            val databaseStore = driver.getKeyValueStore<DatabaseId, Database>(Topology.DATABASE_STORE)
            val databaseNameStore = driver.getKeyValueStore<DatabaseName, DatabaseId>(Topology.DATABASE_NAME_LOOKUP)
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val event = service.deleteDatabase(databaseName)

            event.map {
                val database = databaseStore.get(it.databaseId)
                Assertions.assertNull(database)

                Assertions.assertNull(databaseNameStore.get(databaseName))

                Assertions.assertTrue(
                    driver.producedTopicNames().contains("databases")
                )
                Assertions.assertTrue(
                    driver.producedTopicNames().contains("internal-events")
                )
                Assertions.assertTrue(
                    driver.producedTopicNames().contains("database-names")
                )
            }
        }
}