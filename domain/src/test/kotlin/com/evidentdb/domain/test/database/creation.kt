package com.evidentdb.domain.test.database

import com.evidentdb.domain.Database
import com.evidentdb.domain.DatabaseId
import com.evidentdb.domain.DatabaseNameAlreadyExistsError
import com.evidentdb.domain.InvalidDatabaseNameError
import com.evidentdb.domain.test.InMemoryService
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class CreationTests {
    @Test
    fun `reject a database creation proposal due to invalid database name`(): Unit =
        runBlocking {
            val service = InMemoryService.empty()
            val result = service.createDatabase("")
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is InvalidDatabaseNameError) }
        }

    @Test
    fun `reject a database creation proposal due to already existing name`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val database = Database(DatabaseId.randomUUID(), databaseName)
            val service = InMemoryService(mapOf(Pair(database.id, database)), mapOf())
            val result = service.createDatabase(databaseName)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNameAlreadyExistsError) }
        }

    @Test
    fun `accept a database creation proposal`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val service = InMemoryService.empty()
            val result = service.createDatabase(databaseName)
            Assertions.assertTrue(result.isRight())
            result.map { Assertions.assertEquals(it.data.database.name, databaseName) }
        }
}