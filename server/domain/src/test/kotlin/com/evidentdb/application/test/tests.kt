package com.evidentdb.application.test

import com.evidentdb.adapter.in_memory.InMemoryAdapter
import com.evidentdb.domain_model.BatchConstraint
import com.evidentdb.domain_model.Database
import com.evidentdb.domain_model.ProposedEvent
import io.cloudevents.core.builder.CloudEventBuilder
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.net.URI

class ApplicationTests {
    private val DATABASE_NAME = "foo"
    private val adapter = InMemoryAdapter()
    private val creationResult = runBlocking { adapter.createDatabase(DATABASE_NAME) }

    @Test
    fun `creating a database`() = runBlocking {
        Assertions.assertTrue(creationResult.isRight())
        Assertions.assertInstanceOf(Database::class.java, creationResult.getOrNull())
        val catalog = adapter.catalog().toList()
        Assertions.assertEquals(
            listOf(DATABASE_NAME),
            catalog.map { it.name.value }
        )
    }

    @Test
    fun `transacting batches`() = runBlocking {
        val events = listOf(
            ProposedEvent(
                CloudEventBuilder.v1()
                    .withSource(URI("my-stream"))
                    .withId("a nice id@foo.com")
                    .withSubject("a nice subject")
                    .withType("com.evidentdb.test-event")
                    .build()
            )
        )
        val constraints = listOf<BatchConstraint>()
        val result = adapter.transactBatch(DATABASE_NAME, events, constraints)
        Assertions.assertTrue(result.isRight())
        val catalog = adapter.catalog().toList()
        Assertions.assertEquals(
            listOf<Database>(),
            catalog
        )
    }

    @Test
    fun `querying various streams`() {

    }

    @Test
    fun `deleting a database`() = runBlocking {
        val result = adapter.deleteDatabase(DATABASE_NAME)
        Assertions.assertTrue(result.isRight())
        val catalog = adapter.catalog().toList()
        Assertions.assertEquals(
            listOf<Database>(),
            catalog
        )
    }
}
