package com.evidentdb.database.domain

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.*

class CreationTests {
    @Test
    fun `fail fast on invalid database name`() {
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateCreationProposal(DomainTests.tooShort)
        }

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateCreationProposal(DomainTests.tooLong)
        }

        Assertions.assertDoesNotThrow {
            validateCreationProposal(DomainTests.justRight)
        }
    }

    @Test
    fun `accept a database creation proposal`() {
        val catalog = MockCatalog(1, mapOf())
        val proposal = validateCreationProposal("foo")
        when(val outcome = processCreationProposal(catalog, proposal)) {
            is CreationProposalOutcome.Accepted ->
                Assertions.assertEquals(outcome.event(), DatabaseCreated(proposal.id, proposal.name, catalog.nextRevision()))
            else -> Assertions.fail("Outcome should have been Accepted!")
        }
    }

    @Test
    fun `reject a database creation proposal due to already existing name`() {
        val database = Database(UUID.randomUUID(), "foo")
        val catalog = MockCatalog(1, mapOf(Pair(database.name, database)))
        val proposal = validateCreationProposal("foo")
        when(val outcome = processCreationProposal(catalog, proposal)) {
            is CreationProposalOutcome.Rejected ->
                Assertions.assertEquals(outcome.event(), DatabaseRejected(proposal.id, proposal.name, catalog.nextRevision(), "Database name already exists"))
            else -> Assertions.fail("Outcome should have been Rejected!")
        }
    }
}