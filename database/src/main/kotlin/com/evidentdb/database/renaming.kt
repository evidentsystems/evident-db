package com.evidentdb.database

import java.util.*

data class RenamingProposal(val oldName: String, val newName: String)
data class ValidRenamingProposal(val id: UUID, val newName: String)

fun validateRenamingProposal(catalog: Catalog, proposal: RenamingProposal) : ValidRenamingProposal {
    if (catalog.databases.containsKey(proposal.newName)) throw RuntimeException("Database already exists with same name: ${proposal.newName}")
    val db = catalog.databases[proposal.oldName] ?: throw IllegalStateException("No database named ${proposal.oldName} exists!")
    return ValidRenamingProposal(db.id, proposal.newName)
}