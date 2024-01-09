package com.evidentdb.transactor.test.database

import com.evidentdb.domain_model.DatabaseSummary
import com.evidentdb.domain_model.DatabaseName
import com.evidentdb.domain_model.command.DatabaseNotFoundError
import com.evidentdb.transactor.TransactorTopology
import com.evidentdb.transactor.test.TopologyTestDriverCommandService
import com.evidentdb.transactor.test.driver
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class DeletionTests {
    @Test
    fun `reject a database deletion proposal due to no database with name existing`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = "foo"

            val result = service.deleteDatabase(databaseName)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
        }

    @Test
    fun `topology deletes a database`(): Unit =
        runBlocking {
            val driver = driver()
            val databaseStore = driver.getKeyValueStore<DatabaseName, DatabaseSummary>(TransactorTopology.DATABASE_STORE)
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val event = service.deleteDatabase(databaseName)

            event.map {
                Assertions.assertNull(databaseStore.get(it.database))

                Assertions.assertTrue(
                    driver.producedTopicNames().contains("internal-events")
                )
            }
        }
}