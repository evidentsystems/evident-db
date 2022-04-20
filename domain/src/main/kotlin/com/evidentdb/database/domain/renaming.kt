package com.evidentdb.database.domain

import java.util.*

// TODO: Input-gates via constructors

data class ProposedRenaming(val oldName: String, val newName: String)
data class DatabaseRenamed(val id: UUID,
                           val oldName: String,
                           val newName: String,
                           val revision: Long)
data class DatabaseRenameRejected(val oldName: String,
                                  val newName: String,
                                  val revision: Long,
                                  val reason: String)

sealed class RenamingProposalOutcome {
    class Accepted(private val proposal: ProposedRenaming,
                   private val id: UUID,
                   private val revision: Long) : RenamingProposalOutcome() {
        fun event() = DatabaseRenamed(id, proposal.oldName, proposal.newName, revision)
    }

    class Rejected(private val proposal: ProposedRenaming,
                   private val revision: Long,
                   private val reason: String) : RenamingProposalOutcome() {
        fun event() = DatabaseRenameRejected(proposal.oldName, proposal.newName, revision, reason)
    }
}

fun validateRenamingProposal(oldName: String, newName: String) : ProposedRenaming {
    validateDatabaseName(newName)
    if (oldName === newName)
        throw IllegalArgumentException("When renaming a database, the new name must not match the old name")
    return ProposedRenaming(oldName, newName)
}

fun processRenamingProposal(catalog: Catalog, proposal: ProposedRenaming) : RenamingProposalOutcome {
    val db = catalog.databases[proposal.oldName]
    return if (db == null) {
        RenamingProposalOutcome.Rejected(
            proposal, catalog.nextRevision(),
            "No database named ${proposal.oldName} exists!"
        )
    } else if (catalog.exists(proposal.newName)) {
        RenamingProposalOutcome.Rejected(
            proposal, catalog.nextRevision(),
            "Database already exists with same name: ${proposal.newName}")
    } else {
        RenamingProposalOutcome.Accepted(proposal, db.id, catalog.nextRevision())
    }
}