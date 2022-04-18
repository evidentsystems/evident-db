package com.evidentdb.database.workflows

import com.evidentdb.database.Catalog
import com.evidentdb.database.validateDatabaseName
import java.util.*

data class ProposedRenaming(val oldName: String, val newName: String)
data class DatabaseRenamed(val id: UUID, val oldName: String, val newName: String, val t: Long)
data class DatabaseRenameRejected(val oldName: String, val newName: String, val t: Long, val reason: String)

sealed class RenamingProposalOutcome {
    class Accepted(private val proposal: ProposedRenaming,
                   private val id: UUID,
                   private val t: Long) : RenamingProposalOutcome() {
        fun event() : DatabaseRenamed {
            return DatabaseRenamed(id, proposal.oldName, proposal.newName, t)
        }
    }

    class Rejected(private val proposal: ProposedRenaming,
                   private val t: Long,
                   private val reason: String) : RenamingProposalOutcome() {
        fun event() : DatabaseRenameRejected {
            return DatabaseRenameRejected(proposal.oldName, proposal.newName, t, reason)
        }
    }
}

fun validateRenamingProposal(oldName: String, newName: String) : ProposedRenaming {
    validateDatabaseName(newName)
    if (oldName === newName) throw IllegalArgumentException("When renaming a database, the new name must not match the old name")
    return ProposedRenaming(oldName, newName)
}

fun processRenamingProposal(catalog: Catalog, proposal: ProposedRenaming) : RenamingProposalOutcome {
    val db = catalog.databases[proposal.oldName]
    return if (db == null) {
        RenamingProposalOutcome.Rejected(
            proposal, catalog.nextT(),
            "No database named ${proposal.oldName} exists!"
        )
    } else if (catalog.containsName(proposal.newName)) {
        RenamingProposalOutcome.Rejected(
            proposal, catalog.nextT(),
            "Database already exists with same name: ${proposal.newName}")
    } else {
        RenamingProposalOutcome.Accepted(proposal, db.id, catalog.nextT())
    }
}