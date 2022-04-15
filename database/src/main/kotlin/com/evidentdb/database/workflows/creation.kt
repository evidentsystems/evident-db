package com.evidentdb.database.workflows

import com.evidentdb.database.Catalog
import java.util.*

data class ProposedDatabase(val id: UUID, val name: String)
data class DatabaseCreated(val id: UUID, val name: String, val t: Long)
data class DatabaseRejected(val id: UUID, val name: String, val t: Long)

sealed class ProposalOutcome {
    class Accepted(private val proposal: ProposedDatabase, private val t: Long) : ProposalOutcome() {
        fun event() : DatabaseCreated {
            return DatabaseCreated(proposal.id, proposal.name, t)
        }
    }

    class Rejected(private val proposal: ProposedDatabase, private val t: Long) : ProposalOutcome() {
        fun event() : DatabaseRejected {
            return DatabaseRejected(proposal.id, proposal.name, t)
        }
    }
}

// Input Gate
fun validateCreationProposal(name: String) : ProposedDatabase {
    if (name.length <= 1 || name.length > 100) throw IllegalArgumentException("Database names must be between 1 and 100 characters")
    return ProposedDatabase(UUID.randomUUID(), name)
}

fun processCreationProposal(catalog: Catalog, proposedDatabase: ProposedDatabase) : ProposalOutcome {
    val nextT = catalog.t + 1
    return if (catalog.containsName(proposedDatabase.name)) {
        ProposalOutcome.Rejected(proposedDatabase, nextT)
    } else {
        ProposalOutcome.Accepted(proposedDatabase, nextT)
    }
}
