package com.evidentdb.domain.test.database

import com.evidentdb.domain.Database
import com.evidentdb.domain.DatabaseId
import com.evidentdb.domain.DatabaseNotFoundError
import com.evidentdb.domain.test.InMemoryService
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class DeletionTests {
    @Test
    fun `reject a database deletion proposal due to no database with name existing`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val service = InMemoryService.empty()
            val result = service.deleteDatabase(databaseName)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
        }

    @Test
    fun `accept a database deletion proposal`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val database = Database(DatabaseId.randomUUID(), databaseName)
            val service = InMemoryService(
                mapOf(Pair(database.id, database)),
                mapOf()
            )
            val result = service.deleteDatabase(databaseName)
            Assertions.assertTrue(result.isRight())
            result.map { Assertions.assertEquals(it.data.name, databaseName) }
        }
}