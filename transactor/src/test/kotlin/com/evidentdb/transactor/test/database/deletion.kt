package com.evidentdb.transactor.test.database

import com.evidentdb.domain.Database
import com.evidentdb.domain.DatabaseId
import com.evidentdb.domain.DatabaseName
import com.evidentdb.domain.DatabaseNotFoundError
import com.evidentdb.transactor.Topology
import com.evidentdb.transactor.test.TopologyTestDriverService
import com.evidentdb.transactor.test.driver
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class DeletionTests {
    @Test
    fun `reject a database deletion proposal due to no database with name existing`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"

            val result = service.deleteDatabase(databaseName)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
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
                Assertions.assertNull(databaseStore.get(it.databaseId))
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