package com.evidentdb.database

import java.util.*

data class ProposedDatabase(val id: UUID, val name: String)
data class DatabaseCreated(val id: UUID, val name: String, val t: Long)
data class DatabaseRejected(val id: UUID, val name: String, val t: Long)
sealed class ProposalOutcome {
    object Accepted : ProposalOutcome() {
        fun event(id: UUID, name: String, t: Long) : DatabaseCreated {
            return DatabaseCreated(id, name, t)
        }
    }

    object Rejected : ProposalOutcome() {
        fun event(id: UUID, name: String, t: Long) : DatabaseRejected {
            return DatabaseRejected(id, name, t)
        }
    }
}

// Input Gate
fun proposeDatabase(name: String) : ProposedDatabase {
    if (name.length <= 1 || name.length > 100) throw IllegalArgumentException("Database names must be between 1 and 100 characters")
    return ProposedDatabase(UUID.randomUUID(), name)
}

fun processDatabaseProposal(catalog: Catalog, proposedDatabase: ProposedDatabase) : ProposalOutcome {
    return if (catalog.containsName(proposedDatabase.name)) {
        ProposalOutcome.Rejected
    } else {
        ProposalOutcome.Accepted
    }
}