package com.evidentdb.database.domain

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class DomainTests {
    companion object {
        const val tooShort = ""
        const val tooLong =
            "abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz"
        const val justRight = "a nice short string name"
    }

    @Test
    fun `database name validation predicate`() {
        Assertions.assertFalse(isValidDatabaseName(tooShort))
        Assertions.assertFalse(isValidDatabaseName(tooLong))
        Assertions.assertTrue(isValidDatabaseName(justRight))
    }

    @Test
    fun `validate database name via IllegalArgumentException`() {
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateDatabaseName(tooShort)
        }

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateDatabaseName(tooLong)
        }

        Assertions.assertDoesNotThrow {
            validateDatabaseName(justRight)
        }
    }
}
