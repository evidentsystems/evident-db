package com.evidentdb.adapter.in_memory.test

import com.evidentdb.adapter.in_memory.InMemoryAdapter
import com.evidentdb.adapter.tests.AdapterTests
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class InMemoryAdapterTests: AdapterTests {
    override val databaseName = "foo"
    override val adapter = InMemoryAdapter()
    override val creationResult = runBlocking { adapter.createDatabase(databaseName) }

    @Test
    override fun `creating a database`() {
        super.`creating a database`()
    }

    @Test
    override fun `transacting batches`() {
        super.`transacting batches`()
    }
    @Test
    override fun `querying various streams`() {
        super.`querying various streams`()
    }

    @Test
    override fun `deleting a database`() {
        super.`deleting a database`()
    }
}