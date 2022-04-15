package com.evidentdb.database

import java.util.*

data class Database(val id: UUID, val name: String)

interface Catalog {
    val t: Long
    val databases : Map<String, Database>

    fun containsName(name: String) : Boolean {
        return databases.containsKey(name)
    }
}
