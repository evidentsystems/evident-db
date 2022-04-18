package com.evidentdb.database

import java.util.*

data class Database(val id: UUID, val name: String)

interface Catalog {
    val t: Long
    val databases : Map<String, Database>

    fun containsName(name: String) : Boolean {
        return databases.containsKey(name)
    }

    fun nextT() : Long {
        return t + 1
    }
}

fun isValidDatabaseName(name: String) : Boolean {
    return name.isNotEmpty() && name.length <= 100
}

fun validateDatabaseName(name: String) {
    if (!isValidDatabaseName(name)) throw IllegalArgumentException("Database names must be between 1 and 100 characters")
}