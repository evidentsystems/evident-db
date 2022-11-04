package com.evidentdb.app
import io.micronaut.runtime.EmbeddedApplication
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest
class AppTest {

    @Inject
    lateinit var application: EmbeddedApplication<*>

    @Test
    fun testItWorks() {
        Assertions.assertTrue(application.isRunning)
    }

    @Test
    fun testEvidentDbConfig() {
        val env = application.applicationContext.environment
        println(env.getProperties("evidentdb"))
        Assertions.assertEquals(
            "evidentdb-default-tenant",
            env.get("evidentdb.tenant", String::class.java).get()
        )
    }

}
