package com.evidentdb.database

import com.evidentdb.database.workflows.validateCreationProposal
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class DomainTests {
    companion object {
        val tooShort = ""
        val tooLong =
            "abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz"
        val justRight = "a nice short string name"
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
