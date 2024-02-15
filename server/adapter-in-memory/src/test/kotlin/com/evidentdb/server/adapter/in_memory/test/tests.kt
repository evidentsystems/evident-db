package com.evidentdb.server.adapter.in_memory.test

import com.evidentdb.server.adapter.in_memory.InMemoryAdapter
import com.evidentdb.server.adapter.tests.AdapterTests
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.*

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InMemoryAdapterTests: AdapterTests {
    override val databaseName = "foo"
    override val adapter = InMemoryAdapter()
    override val creationResult = runBlocking { adapter.createDatabase(databaseName) }

    @Test
    @Order(1)
    override fun `creating a database`() {
        super.`creating a database`()
    }

    @Test
    @Order(2)
    override fun `transacting batches`() {
        super.`transacting batches`()
    }

    @Test
    @Order(3)
    override fun `event ID + stream-source uniqueness failure`() {
        super.`event ID + stream-source uniqueness failure`()
    }

    @Test
    @Order(4)
    override fun `batch constraint failures`() {
        super.`batch constraint failures`()
    }

    @Test
    @Order(5)
    override fun `deleting a database`() {
        super.`deleting a database`()
    }
}