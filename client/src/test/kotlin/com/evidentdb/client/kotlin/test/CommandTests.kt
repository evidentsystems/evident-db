package com.evidentdb.client.kotlin.test

import com.evidentdb.client.EvidentDB
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

private const val DATABASE_NAME = "kotlin-client-test-database"

internal class CommandTests {
    private val client = EvidentDB.kotlinClient(
        ManagedChannelBuilder
            .forAddress("localhost", 50051)
            .usePlaintext()
    )

    @Test
    fun `database creation happy path`() {
        runBlocking {
            assert(client.createDatabase(DATABASE_NAME))
        }
    }
}
