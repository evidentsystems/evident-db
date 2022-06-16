package com.evidentdb.domain.test.database

import com.evidentdb.domain.*
import com.evidentdb.domain.test.InMemoryService
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class RenamingTests {
    @Test
    fun `fail fast on invalid database name`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val database = Database(DatabaseId.randomUUID(), databaseName)
            val service = InMemoryService(
                mapOf(Pair(database.id, database)),
                mapOf()
            )
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
            val database1 = Database(DatabaseId.randomUUID(), database1Name)
            val database2Name = "bar"
            val database2 = Database(DatabaseId.randomUUID(), database2Name)
            val service = InMemoryService(
                mapOf(
                    Pair(database1.id, database1),
                    Pair(database2.id, database2),
                ),
                mapOf()
            )
            val result = service.renameDatabase(database1Name, database2Name)
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNameAlreadyExistsError) }
        }

    @Test
    fun `reject a database renaming proposal due to no database existing having given old name`(): Unit =
        runBlocking {
            val service = InMemoryService.empty()
            val result = service.renameDatabase("foo", "bar")
            Assertions.assertTrue(result.isLeft())
            result.mapLeft { Assertions.assertTrue(it is DatabaseNotFoundError) }
        }

    @Test
    fun `accept a database renaming proposal`(): Unit =
        runBlocking {
            val databaseName = "foo"
            val database = Database(DatabaseId.randomUUID(), databaseName)
            val service = InMemoryService(
                mapOf(Pair(database.id, database)),
                mapOf()
            )
            val result = service.renameDatabase(databaseName, "bar")
            Assertions.assertTrue(result.isRight())
            result.map {
                Assertions.assertEquals(it.data.newName, "bar")
            }
        }
}