package com.evidentdb.examples.autonomo
import io.micronaut.http.HttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.runtime.EmbeddedApplication
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals

@MicronautTest
class ApplicationTest {

    @Inject
    lateinit var application: EmbeddedApplication<*>

    @Inject
    lateinit var server: EmbeddedServer

    @Inject
    @field:Client("/rides")
    lateinit var client: HttpClient

    @Test
    fun testAppRuns() {
        Assertions.assertTrue(application.isRunning)
    }

    @Test
    fun testRequestRide() {
        try {
            val rsp: String = client.toBlocking()
                .retrieve(
                    HttpRequest
                        .POST("/", "{" +
                                "\"rider\":\"b4260594-0eb7-4790-89e4-543d25d22e83\"," +
                                "\"origin\":{\"lat\":39.0968646,\"lng\":-94.5790834}," +
                                "\"destination\":{\"lat\":39.0949603,\"lng\":-94.5816267}," +
                                "\"pickupTime\":\"2022-10-31T09:00:00Z\"" +
                                "}")
                        .accept("application/json")
                        .contentType("application/json")
                )
            assertEquals("Hello World", rsp) // (4)
        } catch (error: Exception) {
            println(error)
        }
    }
}
