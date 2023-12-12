package com.evidentdb.domain_model

import arrow.core.*
import arrow.core.raise.*
import com.evidentdb.cloudevents.RecordedTimeExtension
import com.evidentdb.cloudevents.SequenceExtension
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.provider.ExtensionProvider
import java.net.URI
import java.time.Instant
import java.util.*

const val NAME_PATTERN = """^[a-zA-Z][a-zA-Z0-9\-_.]{0,127}$"""
const val DB_URI_SCHEME = "evidentdb"

typealias Revision = ULong

// Database (The Aggregate Root)

typealias DatabaseRevision = Revision

@JvmInline
value class DatabaseSubscriptionURI(val value: URI)

@JvmInline
value class DatabaseName private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidDatabaseName, DatabaseName> = either {
            ensure(value.matches(Regex(NAME_PATTERN))) { InvalidDatabaseName(value) }
            DatabaseName(value)
        }
    }
}

interface Database {
    val name: DatabaseName
    val subscriptionURI: DatabaseSubscriptionURI
    val created: Instant
    val revision: DatabaseRevision
}

data class DatabaseSummary(
    override val name: DatabaseName,
    override val subscriptionURI: DatabaseSubscriptionURI,
    override val created: Instant,
    override val revision: DatabaseRevision
): Database

// Streams

typealias StreamRevision = Revision

@JvmInline
value class StreamName private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidStreamName, StreamName> = either {
            ensure(value.matches(Regex(NAME_PATTERN))) { InvalidStreamName(value) }
            StreamName(value)
        }
    }
}

sealed interface StreamState {
    object NoStream: StreamState
    data class AtRevision(val revision: StreamRevision): StreamState
    data class SubjectAtRevision(val subject: EventSubject, val revision: StreamRevision): StreamState
}

// Event

typealias EventRevision = Revision

@JvmInline
value class EventId private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidEventId, EventId> = either {
            ensure(value.matches(Regex(NAME_PATTERN))) { InvalidEventId(value) }
            EventId(value)
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

/**
 *  Events begin as ProposedEvents. We don't initially know if this event is
 *  a) well-formed or b) has a unique source + ID globally and within its batch.
 *
 * */
data class ProposedEvent(val event: CloudEvent) {
    val uniqueKey = Pair(event.source.toString(), event.id)

    fun ensureWellFormed(batch: ProposedBatch): Either<InvalidEvent, WellFormedProposedEvent> =
        WellFormedProposedEvent(this, batch)
            .mapLeft { InvalidEvent(event, it) }
}

data class WellFormedProposedEvent private constructor (val event: CloudEvent) {
    val stream: StreamName
        get() = event.source.path.split('/').last().let { name ->
            StreamName(name).getOrElse {
                throw IllegalStateException("Illegal event source (stream name): $name")
            }
        }
    val id: EventId
        get() = EventId(event.id).getOrElse { throw IllegalStateException("Illegal event id: $event.id") }

    companion object {
        /**
         *  Given a proposed event, its proposed batch, and a base event source URI
         *  event's source + ID is duplicated within its batch, return either a NEL of EventInvalidations
         *  or a WellFormedProposedEvent.
         *
         *  The stream is provided by the client in the source attribute as a relative URI path,
         *  e.g. 'my-stream', which we'll transform to be relative to the fully qualified
         *  database URI.
         */
        operator fun invoke(
            proposed: ProposedEvent,
            batch: ProposedBatch,
        ): Either<NonEmptyList<EventInvalidation>, WellFormedProposedEvent> = either {
            zipOrAccumulate(
                {
                    val proposedEventSourceURI = ProposedEventSourceURI(proposed.event.source).bind()
                    val streamName = proposedEventSourceURI.toStreamName().bind()
                    batch.databasePathEventSourceURI.toEventSourceURI(streamName).bind()
                },
                { EventId(proposed.event.id).bind() },
                { EventType(proposed.event.type).bind() },
                { proposed.event.subject?.let { EventSubject(it).bind() } },
                {
                    ensure(batch.eventKeyIsUniqueInBatch(proposed)) {
                        DuplicateEventId(proposed.event.source.toString(), proposed.event.id)
                    }
                }
            ) { source, _, _, _, _ ->
                val newEvent = CloudEventBuilder
                    .from(proposed.event)
                    .withSource(source.value)
                    .build()
                WellFormedProposedEvent(newEvent)
            }
        }
    }

    fun accept(
        database: ActiveDatabaseCommandModel,
        indexInBatch: UInt,
        timestamp: Instant
    ): Either<InvalidEvent, AcceptedEvent> =
        AcceptedEvent(this, database, indexInBatch, timestamp)
            .mapLeft { InvalidEvent(event, it) }
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

@JvmInline
value class EventType private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidEventType, EventType> = either {
            ensure(value.matches(Regex(NAME_PATTERN))) { InvalidEventType(value) }
            EventType(value)
        }
    }
}

@JvmInline
value class EventSubject private constructor(val value: String) {
    companion object {
        operator fun invoke(value: String): Either<InvalidEventSubject, EventSubject> = either {
            ensure(value.matches(Regex(NAME_PATTERN))) { InvalidEventSubject(value) }
            EventSubject(value)
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

interface Batch {
    val database: DatabaseName
    val eventRevisions: NonEmptyList<EventRevision>
    val timestamp: Instant
    val previousRevision: DatabaseRevision
    val revision: DatabaseRevision
        get() = previousRevision + eventRevisions.size.toUInt()
}
