package com.evidentdb.database

import com.evidentdb.database.workflows.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.*

data class MockCatalog(override val t: Long, override val databases: Map<String, Database>) : Catalog

class RenamingTests {
    @Test
    fun `accept a database renaming proposal`() {
        val database = Database(UUID.randomUUID(), "foo")
        val catalog = MockCatalog(1, mapOf(Pair(database.name, database)))
        val proposal = validateRenamingProposal(database.name, "bar")
        when(val outcome = processRenamingProposal(catalog, proposal)) {
            is RenamingProposalOutcome.Accepted ->
                Assertions.assertEquals(outcome.event(),
                    DatabaseRenamed(
                        database.id,
                        proposal.oldName,
                        proposal.newName,
                        catalog.t + 1))
            else -> Assertions.fail("Outcome should have been Accepted!")
        }
    }

    @Test
    fun `fail fast on invalid database name`() {
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateRenamingProposal("foo", DomainTests.tooLong)
        }

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateRenamingProposal("foo", DomainTests.tooShort)
        }

        Assertions.assertDoesNotThrow {
            validateRenamingProposal("foo", DomainTests.justRight)
        }
    }

    @Test
    fun `reject a database renaming proposal due to already existing name`() {
        val database = Database(UUID.randomUUID(), "foo")
        val catalog = MockCatalog(1,
            mapOf(Pair(database.name, database),
            Pair("bar", Database(UUID.randomUUID(), "bar"))))
        val proposal = validateRenamingProposal(database.name, "bar")
        when(val outcome = processRenamingProposal(catalog, proposal)) {
            is RenamingProposalOutcome.Rejected ->
                Assertions.assertEquals(outcome.event(),
                    DatabaseRenameRejected(proposal.oldName,
                        proposal.newName,
                        catalog.t + 1,
                        "Database already exists with same name: ${proposal.newName}"))
            else -> Assertions.fail("Outcome should have been Rejected!")
        }
    }

    @Test
    fun `reject a database renaming proposal due to no database existing having given old name`() {
        val catalog = MockCatalog(1,
            mapOf(Pair("bar", Database(UUID.randomUUID(), "bar"))))
        val proposal = validateRenamingProposal("foo", "quux")
        when(val outcome = processRenamingProposal(catalog, proposal)) {
            is RenamingProposalOutcome.Rejected ->
                Assertions.assertEquals(outcome.event(),
                    DatabaseRenameRejected(proposal.oldName,
                        proposal.newName,
                        catalog.t + 1,
                        "No database named ${proposal.oldName} exists!"))
            else -> Assertions.fail("Outcome should have been Rejected!")
        }
    }
}