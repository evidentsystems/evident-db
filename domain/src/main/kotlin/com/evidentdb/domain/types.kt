package com.evidentdb.domain

import java.net.URI
import java.time.Instant
import java.util.*

// Internal Command & Event wrappers

typealias CommandId = UUID
typealias CommandType = String

sealed interface InternalCommand<T> {
    val id: CommandId
    val type: CommandType
        get() = this.javaClass.name
    val databaseId: DatabaseId
    val data: T
}

typealias EventId = UUID
typealias EventType = String

sealed interface InternalEvent<T> {
    val id: EventId
    val type: CommandType
        get() = this.javaClass.name
    val databaseId: DatabaseId
    val data: T
}

// Databases & Catalogs

typealias DatabaseId = UUID
typealias DatabaseName = String
typealias DatabaseRevision = Long

data class Database(val id: DatabaseId,
                    val name: DatabaseName,
                    val created: DatabaseRevision,
                    val revision: DatabaseRevision)

typealias Catalog = Map<String, Database>

// Validated
data class DatabaseCreationInfo(val name: DatabaseName)

data class CreateDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseCreationInfo
): InternalCommand<DatabaseCreationInfo>

sealed interface DatabaseCreationError {
    data class NameAlreadyExists(val name: DatabaseName): DatabaseCreationError
    data class InvalidName(val name: DatabaseName): DatabaseCreationError
}

data class DatabaseCreated(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: Database
): InternalEvent<Database>

data class DatabaseRenameInfo(
    val oldName: DatabaseName,
    val newName: DatabaseName
)

data class RenameDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseRenameInfo
): InternalCommand<DatabaseRenameInfo>

sealed interface DatabaseRenameError {
    data class DatabaseNotFound(val oldName: DatabaseName) : DatabaseRenameError
    data class InvalidNewName(val newName: DatabaseName) : DatabaseRenameError
    data class NameConflict(val renameInfo: DatabaseRenameInfo) : DatabaseRenameError
}

data class DatabaseRenamed(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: DatabaseRenameInfo
): InternalEvent<DatabaseRenameInfo>

data class DatabaseDeletionInfo(
    val name: DatabaseName
)

data class DeleteDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseDeletionInfo
): InternalCommand<DatabaseDeletionInfo>

sealed interface DatabaseDeletionError {
    data class DatabaseNotFound(val name: DatabaseName) : DatabaseDeletionError
}

data class DatabaseDeleted(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: DatabaseDeletionInfo
): InternalEvent<DatabaseDeletionInfo>

// Streams

typealias StreamName = String
typealias StreamRevision = Long
typealias StreamEntityId = String

sealed interface StreamState {
    object Any : StreamState
    object NoStream : StreamState
    object StreamExists : StreamState
    data class AtRevision(val revision: StreamRevision): StreamState
}

sealed interface Stream {
    val databaseId: DatabaseId
    val name: StreamName
    val revision: StreamRevision

    companion object {
        fun create(databaseId: DatabaseId, name: StreamName, revision: StreamRevision = 0): Stream {
            // TODO: construct either a SimpleStream or an EntityStream depending on parsing the naming rules for entity streams
            TODO()
        }
    }
}

data class SimpleStream(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision
) : Stream

data class EntityStream(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision,
    val entityId: StreamEntityId
): Stream

// External/User Events & Batches

sealed interface EventAttributeValue {
    data class BooleanValue(val value: Boolean) : EventAttributeValue
    data class IntegerValue(val value: Long) : EventAttributeValue
    data class StringValue(val value: String) : EventAttributeValue
    data class UriValue(val value: URI) : EventAttributeValue
    data class UriRefValue(val value: URI) : EventAttributeValue
    data class TimestampValue(val value: Instant)
    data class ByteArrayValue(val value: ByteArray) : EventAttributeValue {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ByteArrayValue

            if (!value.contentEquals(other.value)) return false

            return true
        }

        override fun hashCode(): Int {
            return value.contentHashCode()
        }
    }
}

data class ProposedEvent(val id: EventId,
                         val stream: StreamName,
                         val type: EventType,
                         val state: StreamState,
                         val attributes: Map<String, EventAttributeValue>,
                         val data: ByteArray?) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ProposedEvent

        if (id != other.id) return false
        if (stream != other.stream) return false
        if (type != other.type) return false
        if (state != other.state) return false
        if (attributes != other.attributes) return false
        if (data != null) {
            if (other.data == null) return false
            if (!data.contentEquals(other.data)) return false
        } else if (other.data != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + stream.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + state.hashCode()
        result = 31 * result + attributes.hashCode()
        result = 31 * result + (data?.contentHashCode() ?: 0)
        return result
    }
}

data class Event(val id: EventId,
                 val databaseId: DatabaseId,
                 val stream: StreamName,
                 val type: EventType,
                 val revision: StreamRevision,
                 val attributes: Map<String, EventAttributeValue>,
                 val data: ByteArray?) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Event

        if (id != other.id) return false
        if (databaseId != other.databaseId) return false
        if (stream != other.stream) return false
        if (type != other.type) return false
        if (revision != other.revision) return false
        if (attributes != other.attributes) return false
        if (data != null) {
            if (other.data == null) return false
            if (!data.contentEquals(other.data)) return false
        } else if (other.data != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + databaseId.hashCode()
        result = 31 * result + stream.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + revision.hashCode()
        result = 31 * result + attributes.hashCode()
        result = 31 * result + (data?.contentHashCode() ?: 0)
        return result
    }
}

typealias BatchId = UUID

data class ProposedBatch(
    val id: BatchId,
    val databaseId: DatabaseId,
    val events: Iterable<ProposedEvent>
)

data class Batch(
    val id: BatchId,
    val databaseId: DatabaseId,
    val events: Iterable<Event>
)

data class TransactBatch(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: ProposedBatch
): InternalCommand<ProposedBatch>

typealias StreamStateErrorMapping = Map<ProposedEvent, Stream>

sealed interface BatchTransactionError {
    data class DatabaseNotFound(val databaseId: DatabaseId) : BatchTransactionError
    data class MalformedEvents(val events: Iterable<ProposedEvent>) : BatchTransactionError
    data class StreamState(val errors: StreamStateErrorMapping) : BatchTransactionError
}

data class BatchTransacted(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: Batch
): InternalEvent<Batch>