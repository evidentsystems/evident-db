package com.evidentdb.server.domain_model

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.getOrElse
import arrow.core.raise.catch
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import com.evidentdb.server.cloudevents.RecordedTimeExtension
import com.evidentdb.server.cloudevents.SequenceExtension
import io.cloudevents.CloudEvent
import io.cloudevents.core.provider.ExtensionProvider
import java.net.URI
import java.time.Instant

const val DB_URI_SCHEME = "evidentdb"

typealias Revision = ULong

// Database (The Aggregate Root)

typealias DatabaseRevision = Revision

@JvmInline
value class DatabaseName private constructor(val value: String) {
    companion object {
        val PATTERN = Regex("""^[a-zA-Z][a-zA-Z0-9\-_.]{1,127}$""")

        operator fun invoke(value: String): Either<InvalidDatabaseName, DatabaseName> = either {
            ensure(value.matches(PATTERN)) { InvalidDatabaseName(value) }
            DatabaseName(value)
        }
    }
}

interface Database {
    val name: DatabaseName
    val revision: DatabaseRevision
}

// Streams

typealias StreamRevision = Revision

@JvmInline
value class StreamName private constructor(val value: String) {
    companion object {
        val PATTERN = Regex("""^[a-zA-Z][a-zA-Z0-9\-_.]{1,127}$""")

        operator fun invoke(value: String): Either<InvalidStreamName, StreamName> = either {
            ensure(value.matches(PATTERN)) { InvalidStreamName(value) }
            StreamName(value)
        }
    }
}

// Event

typealias EventRevision = Revision

@JvmInline
value class EventId private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidEventId, EventId> = either {
            ensure(value.isNotEmpty()) { InvalidEventId(value) }
            EventId(value)
        }
    }
}

@JvmInline
value class EventType private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidEventType, EventType> = either {
            ensure(value.isNotEmpty()) { InvalidEventType(value) }
            EventType(value)
        }
    }
}

@JvmInline
value class EventSubject private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidEventSubject, EventSubject> = either {
            ensure(value.isNotEmpty()) { InvalidEventSubject(value) }
            EventSubject(value)
        }
    }
}

@JvmInline
value class ProposedEventSourceURI private constructor(val value: URI) {
    companion object {
        operator fun invoke(uri: URI): Either<InvalidEventSource, ProposedEventSourceURI> = either {
            val err = InvalidEventSource(uri.toString())
            ensure(!uri.isAbsolute) { err }
            StreamName(uri.path).mapLeft { err }.bind()
            ProposedEventSourceURI(uri)
        }
    }

    fun toStreamName(): Either<InvalidStreamName, StreamName> =
        StreamName(value.path)
}

@JvmInline
value class EmptyPathEventSourceURI private constructor(val value: URI) {
    fun toDatabasePathEventSourceURI(databaseName: DatabaseName) =
        DatabasePathEventSourceURI(this, databaseName)
        
    companion object {
        operator fun invoke(uriStr: String): Either<InvalidEventSource, EmptyPathEventSourceURI> = either {
            var maybeUri: URI? = null
            val err = InvalidEventSource(uriStr)
            catch({ maybeUri = URI(uriStr) }) { err }
            val uri = validate(maybeUri).bind()
            ensure(uri.path.isNullOrEmpty()) { err }
            EmptyPathEventSourceURI(uri)
        }

        private fun validate(maybeUri: URI?): Either<InvalidEventSource, URI> = either {
            val err = InvalidEventSource(maybeUri.toString())
            ensureNotNull(maybeUri) { err }
            ensure(maybeUri.isAbsolute) { err }
            ensure(maybeUri.scheme == DB_URI_SCHEME) { err }
            ensure(maybeUri.authority.isNotBlank()) { err }
            maybeUri
        }
    }
}

@JvmInline
value class DatabasePathEventSourceURI private constructor(val value: URI) {
    val databaseName: DatabaseName
        get() = value.path.split('/')[1].let { name ->
            DatabaseName(name).getOrElse {
                throw IllegalStateException("Illegal event source (database name): $name")
            }
        }

    fun toEventSourceURI(streamName: StreamName) =
        EventSourceURI(this, streamName)

    companion object {
        operator fun invoke(
            root: EmptyPathEventSourceURI,
            databaseName: DatabaseName
        ): Either<InvalidEventSource, DatabasePathEventSourceURI> = either {
            DatabasePathEventSourceURI(root.value.resolve("/${databaseName.value}/"))
        }

        operator fun invoke(
            root: EmptyPathEventSourceURI,
            databaseNameStr: String
        ): Either<InvalidEventSource, DatabasePathEventSourceURI> = either {
            val databaseName = DatabaseName(databaseNameStr)
                .mapLeft { InvalidEventSource("database: $databaseNameStr") }
                .bind()
            invoke(root, databaseName).bind()
        }
    }
}

@JvmInline
value class EventSourceURI private constructor(val value: URI) {
    val databaseName: DatabaseName
        get() = value.path.split('/')[1].let { name ->
            DatabaseName(name).getOrElse {
                throw IllegalStateException("Illegal event source (database name): $name")
            }
        }
    val streamName: StreamName
        get() = value.path.split('/')[2].let { name ->
            StreamName(name).getOrElse {
                throw IllegalStateException("Illegal event source (stream name): $name")
            }
        }

    companion object {
        operator fun invoke(
            base: DatabasePathEventSourceURI,
            streamName: StreamName
        ): Either<InvalidEventSource, EventSourceURI> = either {
            EventSourceURI(base.value.resolve(streamName.value))
        }

        operator fun invoke(
            base: DatabasePathEventSourceURI,
            streamNameStr: String
        ): Either<InvalidEventSource, EventSourceURI> = either {
            val streamName = StreamName(streamNameStr)
                .mapLeft { InvalidEventSource("base: $base, stream: $streamNameStr") }
                .bind()
            invoke(base, streamName).bind()
        }
    }
}

interface Event {
    val event: CloudEvent

    val database: DatabaseName
        get() = event.source.path.split('/')[1].let { name ->
            DatabaseName(name).getOrElse {
                throw IllegalStateException("Illegal event source (database name): $name")
            }
        }
    val stream: StreamName
        get() = event.source.path.split('/').last().let { name ->
            StreamName(name).getOrElse {
                throw IllegalStateException("Illegal event source (stream name): $name")
            }
        }
    val id: EventId
        get() = EventId(event.id).getOrElse { throw IllegalStateException("Illegal event id: $event.id") }
    val type: EventType
        get() = EventType(event.type).getOrElse { throw IllegalStateException("Illegal event type: $event.type") }
    val subject: EventSubject?
        get() = event.subject?.let { EventSubject(it).getOrElse { throw IllegalStateException("Illegal event subject: $event.subject") } }
    val revision: EventRevision
        get() = ExtensionProvider.getInstance()
            .parseExtension(SequenceExtension::class.java, event)!!
            .sequence
    val time: Instant?
        get() = event.time?.toInstant()
    val recordedTime: Instant
        get() = ExtensionProvider.getInstance()
            .parseExtension(RecordedTimeExtension::class.java, event)!!
            .recordedTime
}

// Batch

sealed interface BatchConstraint {
    data class StreamExists(val stream: StreamName) : BatchConstraint
    data class StreamDoesNotExist(val stream: StreamName) : BatchConstraint
    data class StreamMaxRevision(val stream: StreamName, val revision: StreamRevision) :
        BatchConstraint

    data class SubjectExists(val subject: EventSubject) : BatchConstraint
    data class SubjectDoesNotExist(val subject: EventSubject) : BatchConstraint
    data class SubjectMaxRevision(
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint

    data class SubjectExistsOnStream(val stream: StreamName, val subject: EventSubject) :
        BatchConstraint
    data class SubjectDoesNotExistOnStream(val stream: StreamName, val subject: EventSubject) :
        BatchConstraint
    data class SubjectMaxRevisionOnStream(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint
}

interface Batch {
    val database: DatabaseName
    val eventRevisions: NonEmptyList<EventRevision>
    val timestamp: Instant
    val basisRevision: DatabaseRevision
    val revision: DatabaseRevision
        get() = basisRevision + eventRevisions.size.toUInt()
}
