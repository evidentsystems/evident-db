package com.evidentdb.adapter.local_file.test

import com.evidentdb.adapter.local_file.LocalFileAdapter
import com.evidentdb.adapter.tests.AdapterTests
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test

class LocalFileAdapterTests: AdapterTests {
    override val databaseName = "foo"
    override val adapter = LocalFileAdapter()
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
    override fun `deleting a database`() {
        super.`deleting a database`()
    }
}