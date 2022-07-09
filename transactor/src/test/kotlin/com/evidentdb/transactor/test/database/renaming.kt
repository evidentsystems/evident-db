package com.evidentdb.transactor.test.database

import com.evidentdb.domain.*
import com.evidentdb.transactor.Topology
import com.evidentdb.transactor.test.TopologyTestDriverService
import com.evidentdb.transactor.test.driver
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class RenamingTests {
    @Test
    fun `fail fast on invalid database name`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)
            val databaseName = "foo"
            service.createDatabase(databaseName)

            val result = service.renameDatabase(databaseName,"")
            Assertions.assertTrue(result.isLeft())
            result.mapLeft {
                println(it)
                Assertions.assertTrue(it is InvalidDatabaseNameError)
            }
        }

    @Test
    fun `reject a database renaming proposal due to already existing name`(): Unit =
        runBlocking {
            val database1Name = "foo"
            val database2Name = "bar"
            val driver = driver()
            val service = TopologyTestDriverService(driver)
            service.createDatabase(database1Name)
            service.createDatabase(database2Name)

            val result = service.renameDatabase(database1Name, database2Name)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNameAlreadyExistsError) }
        }

    @Test
    fun `reject a database renaming proposal due to no database existing having given old name`(): Unit =
        runBlocking {
            val driver = driver()
            val service = TopologyTestDriverService(driver)

            val result = service.renameDatabase("foo", "bar")
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
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
}