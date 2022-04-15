package com.evidentdb.database

import com.evidentdb.database.workflows.ProposedDatabase
import com.evidentdb.database.workflows.ValidRenamingProposal

interface ReadableStore {
    fun byName(name: String) : Database?
    fun catalog() : Catalog
}

interface WriteableStore : ReadableStore {
    fun create(proposal: ProposedDatabase) : Catalog
    fun rename(proposal: ValidRenamingProposal) : Catalog
    fun delete(name: String) : Catalog
}