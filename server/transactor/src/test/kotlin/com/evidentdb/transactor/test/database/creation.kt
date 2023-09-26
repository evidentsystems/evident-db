package com.evidentdb.transactor.test.database

import com.evidentdb.domain.DatabaseSummary
import com.evidentdb.domain.DatabaseName
import com.evidentdb.domain.DatabaseNameAlreadyExistsError
import com.evidentdb.domain.InvalidDatabaseNameError
import com.evidentdb.transactor.TransactorTopology
import com.evidentdb.transactor.test.TopologyTestDriverCommandService
import com.evidentdb.transactor.test.driver
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class CreationTests {
    @Test
    fun `reject a database creation proposal due to invalid database name`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = ""

            val result = service.createDatabase(databaseName)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is InvalidDatabaseNameError) }
        }

    @Test
    fun `reject a database creation proposal due to already existing name`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverCommandService(driver)
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
            val databaseStore = driver.getKeyValueStore<DatabaseName, DatabaseSummary>(TransactorTopology.DATABASE_STORE)
            val service = TopologyTestDriverCommandService(driver)
            val databaseName = "foo"

            val event = service.createDatabase(databaseName)

            Assertions.assertTrue(event.isRight())
            event.map {
                Assertions.assertEquals(
                    it.data.database.name,
                    databaseStore.get(it.database)?.name,
                )

                Assertions.assertTrue(
                    driver.producedTopicNames().contains("internal-events")
                )
            }
        }
}