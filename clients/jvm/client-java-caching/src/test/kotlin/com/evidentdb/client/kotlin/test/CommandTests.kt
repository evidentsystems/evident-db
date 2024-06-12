package com.evidentdb.client.kotlin.test

import com.evidentdb.client.EvidentDb
import io.grpc.ManagedChannelBuilder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

private const val DATABASE_NAME = "kotlin-client-test-database"

internal class CommandTests {

    companion object {
        private val client = EvidentDb.kotlinClient(
            ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()
        )

        @BeforeAll
        @JvmStatic
        internal fun `create database`() {
            client.createDatabase(DATABASE_NAME)
        }

        @AfterAll
        @JvmStatic
        internal fun `delete database`() {
            client.deleteDatabase(DATABASE_NAME)
        }
    }

    @Test
    fun `getting a database from the connection`() {
        val conn = client.connectDatabase(DATABASE_NAME)
        assertEquals(conn.db().name, DATABASE_NAME)
    }
}
