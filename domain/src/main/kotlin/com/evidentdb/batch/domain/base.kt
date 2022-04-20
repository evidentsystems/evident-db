package com.evidentdb.batch.domain

import java.util.*

sealed class StreamState {
    object Any : StreamState()
    object NoStream : StreamState()
    object StreamExists : StreamState()
    class AtRevision(val revision: Long): StreamState()
}

data class Event(val id: UUID, val stream: String, val revision: Long)

interface Index {
    val database: UUID
    val revision: Long

    fun streamExists(stream: String): Boolean
    fun streamRevision(stream: String): Long
}
