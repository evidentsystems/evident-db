package com.evidentdb.transactor.test.database

import com.evidentdb.domain.*
import com.evidentdb.transactor.Topology
import com.evidentdb.transactor.test.TopologyTestDriverService
import com.evidentdb.transactor.test.driver
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class CreationTests {
    @Test
    fun `reject a database creation proposal due to invalid database name`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)

            val result = service.createDatabase("")
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is InvalidDatabaseNameError) }
        }

    @Test
    fun `reject a database creation proposal due to already existing name`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val result = service.createDatabase(databaseName)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNameAlreadyExistsError) }
        }

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
}