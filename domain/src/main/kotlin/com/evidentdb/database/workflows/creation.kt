package com.evidentdb.database.workflows

import com.evidentdb.database.Catalog
import com.evidentdb.database.validateDatabaseName
import java.util.*

data class ProposedDatabase(val id: UUID, val name: String)
data class DatabaseCreated(val id: UUID, val name: String, val t: Long)
data class DatabaseRejected(val id: UUID, val name: String, val t: Long, val reason: String)

sealed class CreationProposalOutcome {
    class Accepted(private val proposal: ProposedDatabase, private val t: Long) : CreationProposalOutcome() {
        fun event() : DatabaseCreated {
            return DatabaseCreated(proposal.id, proposal.name, t)
        }
    }

    class Rejected(private val proposal: ProposedDatabase,
                   private val t: Long,
                   private val reason: String) : CreationProposalOutcome() {
        fun event() : DatabaseRejected {
            return DatabaseRejected(proposal.id, proposal.name, t, reason)
        }
    }
}

fun validateCreationProposal(name: String) : ProposedDatabase {
    validateDatabaseName(name)
    return ProposedDatabase(UUID.randomUUID(), name)
}

fun processCreationProposal(catalog: Catalog, proposedDatabase: ProposedDatabase) : CreationProposalOutcome {
    return if (catalog.containsName(proposedDatabase.name)) {
        CreationProposalOutcome.Rejected(proposedDatabase, catalog.nextT(), "Database name already exists")
    } else {
        CreationProposalOutcome.Accepted(proposedDatabase, catalog.nextT())
    }
}
