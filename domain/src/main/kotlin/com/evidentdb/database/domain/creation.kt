package com.evidentdb.database.domain

import java.util.*

// TODO: Input-gates via constructors

data class ProposedDatabase(val id: UUID, val name: String)
data class DatabaseCreated(val id: UUID, val name: String, val revision: Long)
data class DatabaseRejected(val id: UUID, val name: String, val revision: Long, val reason: String)

sealed class CreationProposalOutcome {
    class Accepted(private val proposal: ProposedDatabase, private val revision: Long)
        : CreationProposalOutcome() {
        fun event() : DatabaseCreated {
            return DatabaseCreated(proposal.id, proposal.name, revision)
        }
    }

    class Rejected(private val proposal: ProposedDatabase,
                   private val revision: Long,
                   private val reason: String) : CreationProposalOutcome() {
        fun event() : DatabaseRejected {
            return DatabaseRejected(proposal.id, proposal.name, revision, reason)
        }
    }
}

fun validateCreationProposal(name: String) : ProposedDatabase {
    validateDatabaseName(name)
    return ProposedDatabase(UUID.randomUUID(), name)
}

fun processCreationProposal(catalog: Catalog, proposedDatabase: ProposedDatabase) =
    if (catalog.exists(proposedDatabase.name)) {
        CreationProposalOutcome.Rejected(
            proposedDatabase,
            catalog.nextRevision(),
            "Database name already exists")
    } else {
        CreationProposalOutcome.Accepted(proposedDatabase, catalog.nextRevision())
    }
