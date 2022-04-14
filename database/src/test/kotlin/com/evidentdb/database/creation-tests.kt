package com.evidentdb.database

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.*

data class MockCatalog(override val t: Long, override val databases: Map<String, Database>) : Catalog

class CreationTests {
    @Test
    fun `database name constraints and input gate`() {
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            proposeDatabase("")
        }

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            proposeDatabase("abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz")
        }

        val name = "a nice short string name"
        val proposal = proposeDatabase(name)
        Assertions.assertEquals(proposal.name, name)
    }

    @Test
    fun `accept a database proposal`() {
        val catalog = MockCatalog(1, mapOf())
        val proposal = proposeDatabase("foo")
        when(val outcome = processDatabaseProposal(catalog, proposal)) {
            is ProposalOutcome.Accepted ->
                Assertions.assertEquals(outcome.event(), DatabaseCreated(proposal.id, proposal.name, catalog.t + 1))
            else -> Assertions.fail("Outcome should have been Accepted!")
        }
    }

    @Test
    fun `reject a database proposal`() {
        val catalog = MockCatalog(1, mapOf(Pair("foo", Database(UUID.randomUUID(), "foo"))))
        val proposal = proposeDatabase("foo")
        when(val outcome = processDatabaseProposal(catalog, proposal)) {
            is ProposalOutcome.Rejected ->
                Assertions.assertEquals(outcome.event(), DatabaseRejected(proposal.id, proposal.name, catalog.t + 1))
            else -> Assertions.fail("Outcome should have been Rejected!")
        }
    }
}