package com.evidentdb.domain.test.database

import com.evidentdb.domain.DatabaseName
import com.evidentdb.domain.DatabaseNotFoundError
import com.evidentdb.domain.test.InMemoryCommandService
import com.evidentdb.domain.test.buildTestDatabase
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class DeletionTests {
    @Test
    fun `reject a database deletion proposal due to no database with name existing`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val service = InMemoryCommandService.empty()
            val result = service.deleteDatabase(databaseName)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
        }

    @Test
    fun `accept a database deletion proposal`(): Unit =
        runBlocking {
            val databaseName = DatabaseName.build("foo")
            val database = buildTestDatabase(databaseName)
            val service = InMemoryCommandService(
                listOf(database),
                listOf(),
                listOf()
            )
            val result = service.deleteDatabase(databaseName.value)
            Assertions.assertTrue(result.isRight())
            result.map { Assertions.assertEquals(it.database, databaseName) }
        }
}