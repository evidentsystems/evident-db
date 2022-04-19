package com.evidentdb.database

import com.evidentdb.batch.Index
import java.util.*

data class Database(val id: UUID, val name: String)

interface Catalog {
    val revision: Long
    val databases: Map<String, Database>

    fun index(id: UUID) : Index

    fun exists(name: String) : Boolean {
        return databases.containsKey(name)
    }

    fun nextRevision() : Long {
        return revision + 1
    }
}

fun isValidDatabaseName(name: String) = name.isNotEmpty() && name.length <= 100

fun validateDatabaseName(name: String) {
    if (!isValidDatabaseName(name)) throw IllegalArgumentException("Database names must be between 1 and 100 characters")
}