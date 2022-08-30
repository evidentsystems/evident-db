package com.evidentdb.domain.test.database

import com.evidentdb.domain.Database
import com.evidentdb.domain.DatabaseName
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
            val databaseName = DatabaseName.build("foo")
            val database = Database(databaseName)
            val service = InMemoryService(listOf(database), listOf())
            val result = service.createDatabase(databaseName.value)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNameAlreadyExistsError) }
        }

    @Test
    fun `accept a database creation proposal`(): Unit =
        runBlocking {
            val databaseName = DatabaseName.build("foo")
            val service = InMemoryService.empty()
            val result = service.createDatabase(databaseName.value)
            Assertions.assertTrue(result.isRight())
            result.map { Assertions.assertEquals(it.data.name, databaseName) }
        }
}