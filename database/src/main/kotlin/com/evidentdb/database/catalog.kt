package com.evidentdb.database

import java.util.*

// Domain Model

data class Database(val id: UUID, val name: String)
interface Catalog {
    val t: Long

    fun containsName(name: String) : Boolean
    fun databases() : Map<String, Database>
}

// Service Interface

interface ReadableStore {
    fun byName(name: String) : Database?
    fun catalog() : Catalog
}

interface WriteableStore : ReadableStore {
    fun create(proposal: ProposedDatabase) : Catalog
    fun rename(proposal: ValidRenamingProposal) : Catalog
    fun delete(name: String) : Catalog
}