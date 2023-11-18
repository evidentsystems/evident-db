package com.evidentdb.domain_model

import arrow.core.*
import com.evidentdb.application.base32HexStringToLong
import com.evidentdb.domain_model.command.EventSubject
import com.evidentdb.domain_model.command.InvalidStreamName
import io.cloudevents.CloudEvent
import org.valiktor.ConstraintViolationException
import org.valiktor.functions.matches
import org.valiktor.i18n.toMessage
import org.valiktor.validate
import java.net.URI
import java.util.*

const val NAME_PATTERN = """^[a-zA-Z][a-zA-Z0-9\-_.]{0,127}$"""
const val DB_URI_SCHEME = "evidentdb"

// Validation

internal inline fun <reified T> valikate(
    validationFn: () -> T
): Either<NonEmptyList<String>, T> = try {
    validationFn().right()
} catch (ex: ConstraintViolationException) {
    ex.constraintViolations
        .map {
            val message = it.toMessage()
            "\"${message.value}\" of ${T::class.simpleName}.${message.property}: ${message.message}"
        }.toNonEmptyListOrNull()!!.left()
}

// Streams & Indexes

typealias StreamRevision = Long

sealed interface StreamState {
    object NoStream: StreamState
    data class AtRevision(val revision: StreamRevision): StreamState
    data class SubjectAtRevision(val subject: EventSubject, val revision: StreamRevision): StreamState
}

@JvmInline
value class StreamName private constructor(val value: String) {
    companion object {
        fun build(value: String): StreamName =
            validate(StreamName(value)) {
                validate(StreamName::value).matches(Regex(NAME_PATTERN))
            }

        fun of(value: String): Either<InvalidStreamName, StreamName> =
            valikate { build(value) }.mapLeft { InvalidStreamName(value) }.toEither()
    }
}

// Event

typealias EventId = UUID
typealias EventIndex = Long
typealias EventKey = String
typealias EventType = String

// TODO: extract DatabaseName and StreamName from CloudEvent
data class Event(
    val database: DatabaseName,
    val stream: StreamName,
    val event: CloudEvent
) {
    val index: EventIndex
        get() = event.getExtension("sequence").let {
            when (it) {
                is String -> base32HexStringToLong(it)
                else -> throw IllegalStateException("Event Sequence must be a String")
            }
        }
    val id: EventId
        get() = UUID.fromString(event.id)
}

// Database (The Aggregate Root)

//fun databaseRevisionFromEvent(event: EventEnvelope): Database? =
//    when(event) {
//        is BatchTransacted -> event.data.databaseAfter
//        is DatabaseCreated -> event.data.database
//        else -> null
//    }

typealias DatabaseRevision = Long
typealias DatabaseLogKey = String
typealias TopicName = String

@JvmInline
value class DatabaseName private constructor(val value: String) {
    fun asStreamKeyPrefix() = "$value/"

    companion object {
        fun build(value: String): DatabaseName =
            validate(DatabaseName(value)) {
                validate(DatabaseName::value).matches(Regex(NAME_PATTERN))
            }

        fun of(value: String): Either<InvalidDatabaseNameError, DatabaseName> =
            valikate { build(value) }.mapLeft { InvalidDatabaseNameError(value) }.toEither()
    }
}


@JvmInline
value class DatabaseClock(val streamRevisions: Map<StreamName, StreamRevision>) {
    companion object {
        val EMPTY = DatabaseClock(mapOf())
    }

    val revision: DatabaseRevision
        get() = streamRevisions.values.fold(0) { acc, streamRevision ->
            acc + streamRevision
        }

    fun advance(events: Iterable<Event>) = DatabaseClock(
        events.fold(streamRevisions.toMutableMap()) { newClock, event ->
            val nextRevision = (newClock[event.stream] ?: 0) + 1
            newClock[event.stream] = nextRevision
            newClock
        }
    )

}

fun databaseUri(name: DatabaseName): URI =
    URI(DB_URI_SCHEME, name.value, null)

fun databaseNameFromUri(uri: URI): DatabaseName =
    DatabaseName.build(uri.schemeSpecificPart)
