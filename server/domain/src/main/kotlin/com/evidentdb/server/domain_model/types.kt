package com.evidentdb.server.domain_model

import arrow.core.Either
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.catch
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import arrow.core.right
import com.evidentdb.server.cloudevents.RecordedTimeExtension
import com.evidentdb.server.cloudevents.SequenceExtension
import io.cloudevents.CloudEvent
import io.cloudevents.core.provider.ExtensionProvider
import java.net.URI
import java.time.Instant

const val DB_URI_SCHEME = "evidentdb"

typealias Revision = ULong

// Database (The Aggregate Root)

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
    val revision: Revision
}

// Streams

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
        get() = EventId(event.id).getOrElse {
            throw IllegalStateException("Illegal event id: $event.id")
        }
    val type: EventType
        get() = EventType(event.type).getOrElse {
            throw IllegalStateException("Illegal event type: $event.type")
        }
    val subject: EventSubject?
        get() = event.subject?.let {
            EventSubject(it).getOrElse {
                throw IllegalStateException("Illegal event subject: $event.subject")
            }
        }
    val revision: Revision
        get() = ExtensionProvider.getInstance()
            .parseExtension(SequenceExtension::class.java, event)!!
            .sequence
    val revisionElen: String
        get() = ExtensionProvider.getInstance()
            .parseExtension(SequenceExtension::class.java, event)!!
            .getValue(SequenceExtension.SEQUENCE_KEY)
    val time: Instant?
        get() = event.time?.toInstant()
    val recordedTime: Instant
        get() = ExtensionProvider.getInstance()
            .parseExtension(RecordedTimeExtension::class.java, event)!!
            .recordedTime
}

// Batch

sealed interface BatchConstraintKey {
    data object Database: BatchConstraintKey
    data class Stream(val stream: StreamName): BatchConstraintKey
    data class Subject(val subject: EventSubject): BatchConstraintKey
    data class SubjectStream(val stream: StreamName, val subject: EventSubject): BatchConstraintKey

    companion object {
        fun from(constraint: BatchConstraint): BatchConstraintKey = when(constraint) {
            is BatchConstraint.DatabaseMinRevision -> Database
            is BatchConstraint.DatabaseMaxRevision -> Database
            is BatchConstraint.DatabaseRevisionRange -> Database

            is BatchConstraint.StreamMinRevision -> Stream(constraint.stream)
            is BatchConstraint.StreamMaxRevision -> Stream(constraint.stream)
            is BatchConstraint.StreamRevisionRange -> Stream(constraint.stream)

            is BatchConstraint.SubjectMinRevision -> Subject(constraint.subject)
            is BatchConstraint.SubjectMaxRevision -> Subject(constraint.subject)
            is BatchConstraint.SubjectRevisionRange -> Subject(constraint.subject)

            is BatchConstraint.SubjectMinRevisionOnStream ->
                SubjectStream(constraint.stream, constraint.subject)
            is BatchConstraint.SubjectMaxRevisionOnStream ->
                SubjectStream(constraint.stream, constraint.subject)
            is BatchConstraint.SubjectStreamRevisionRange ->
                SubjectStream(constraint.stream, constraint.subject)
        }
    }
}

sealed interface BatchConstraint {
    data class DatabaseMinRevision(val revision: Revision) : BatchConstraint
    data class DatabaseMaxRevision(val revision: Revision) : BatchConstraint
    data class DatabaseRevisionRange private constructor(
        val min: Revision,
        val max: Revision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                min: Revision,
                max: Revision,
            ): Either<BatchConstraintInvalidation, DatabaseRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                DatabaseRevisionRange(min, max)
            }
        }
    }

    data class StreamMinRevision(
        val stream: StreamName,
        val revision: Revision
    ) : BatchConstraint
    data class StreamMaxRevision(
        val stream: StreamName,
        val revision: Revision
    ) : BatchConstraint
    data class StreamRevisionRange private constructor(
        val stream: StreamName,
        val min: Revision,
        val max: Revision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                stream: StreamName,
                min: Revision,
                max: Revision,
            ): Either<BatchConstraintInvalidation, StreamRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                StreamRevisionRange(stream, min, max)
            }
        }
    }

    data class SubjectMinRevision(
        val subject: EventSubject,
        val revision: Revision,
    ) : BatchConstraint
    data class SubjectMaxRevision(
        val subject: EventSubject,
        val revision: Revision,
    ) : BatchConstraint
    data class SubjectRevisionRange private constructor(
        val subject: EventSubject,
        val min: Revision,
        val max: Revision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                subject: EventSubject,
                min: Revision,
                max: Revision,
            ): Either<BatchConstraintInvalidation, SubjectRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                SubjectRevisionRange(subject, min, max)
            }
        }
    }

    data class SubjectMinRevisionOnStream(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: Revision,
    ) : BatchConstraint
    data class SubjectMaxRevisionOnStream(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: Revision,
    ) : BatchConstraint
    data class SubjectStreamRevisionRange private constructor(
        val stream: StreamName,
        val subject: EventSubject,
        val min: Revision,
        val max: Revision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                stream: StreamName,
                subject: EventSubject,
                min: Revision,
                max: Revision,
            ): Either<BatchConstraintInvalidation, SubjectStreamRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                SubjectStreamRevisionRange(stream, subject, min, max)
            }
        }
    }

    companion object {
        fun streamExists(stream: StreamName) = StreamMinRevision(stream, 1uL)

        fun streamDoesNotExist(stream: StreamName) = StreamMaxRevision(stream, 0uL)

        fun streamAtRevision(stream: StreamName, revision: Revision) =
            StreamRevisionRange(stream, revision, revision)

        fun subjectExists(subject: EventSubject) = SubjectMinRevision(subject, 1uL)

        fun subjectDoesNotExist(subject: EventSubject) = SubjectMaxRevision(subject, 0uL)

        fun subjectAtRevision(subject: EventSubject, revision: Revision) =
            SubjectRevisionRange(subject, revision, revision)

        fun subjectExistsOnStream(stream: StreamName, subject: EventSubject) =
            SubjectMinRevisionOnStream(stream, subject, 1uL)

        fun subjectDoesNotExistOnStream(stream: StreamName, subject: EventSubject) =
            SubjectMaxRevisionOnStream(stream, subject, 0uL)

        fun subjectAtRevisionOnStream(stream: StreamName, subject: EventSubject, revision: Revision) =
            SubjectStreamRevisionRange(stream, subject, revision, revision)

        fun combine(
            lhs: BatchConstraint?,
            rhs: BatchConstraint
        ): Either<BatchConstraintConflict, BatchConstraint> = if (lhs == null) {
            rhs.right()
        } else {
            val conflict = BatchConstraintConflict(lhs, rhs)
            val err = IllegalStateException("Invalid batch constraint key lookup: $lhs, $rhs")
            when (lhs) {
                is DatabaseMinRevision -> when (rhs) {
                    is DatabaseMaxRevision ->
                        DatabaseRevisionRange(lhs.revision, rhs.revision).mapLeft { conflict }
                    is DatabaseMinRevision,
                    is DatabaseRevisionRange -> conflict.left()
                    else -> throw err
                }
                is DatabaseMaxRevision -> when (rhs) {
                    is DatabaseMinRevision -> DatabaseRevisionRange(rhs.revision, lhs.revision)
                        .mapLeft { conflict }
                    is DatabaseMaxRevision,
                    is DatabaseRevisionRange -> conflict.left()
                    else -> throw err
                }
                is DatabaseRevisionRange -> when (rhs) {
                    is DatabaseMinRevision,
                    is DatabaseMaxRevision,
                    is DatabaseRevisionRange -> conflict.left()
                    else -> throw err
                }

                is StreamMinRevision -> when (rhs) {
                    is StreamMaxRevision -> if (lhs.stream == rhs.stream) {
                        StreamRevisionRange(lhs.stream, lhs.revision, rhs.revision)
                            .mapLeft { conflict }
                    } else {
                        throw err
                    }
                    is StreamMinRevision -> if (lhs.stream == rhs.stream) conflict.left() else throw err
                    is StreamRevisionRange -> if (lhs.stream == rhs.stream) conflict.left() else throw err
                    else -> throw err
                }
                is StreamMaxRevision -> when (rhs) {
                    is StreamMinRevision -> if (lhs.stream == rhs.stream) {
                        StreamRevisionRange(lhs.stream, rhs.revision, lhs.revision)
                            .mapLeft{ conflict }
                    } else {
                        throw err
                    }
                    is StreamMaxRevision -> if (lhs.stream == rhs.stream) conflict.left() else throw err
                    is StreamRevisionRange -> if (lhs.stream == rhs.stream) conflict.left() else throw err
                    else -> throw err
                }
                is StreamRevisionRange -> when (rhs) {
                    is StreamMinRevision -> if (lhs.stream == rhs.stream) conflict.left() else throw err
                    is StreamMaxRevision -> if (lhs.stream == rhs.stream) conflict.left() else throw err
                    is StreamRevisionRange -> if (lhs.stream == rhs.stream) conflict.left() else throw err
                    else -> throw err
                }

                is SubjectMinRevision -> when (rhs) {
                    is SubjectMaxRevision -> if (lhs.subject == rhs.subject) {
                        SubjectRevisionRange(lhs.subject, lhs.revision, rhs.revision)
                            .mapLeft { conflict }
                    } else {
                        throw err
                    }
                    is SubjectMinRevision -> if (lhs.subject == rhs.subject) conflict.left() else throw err
                    is SubjectRevisionRange -> if (lhs.subject == rhs.subject) conflict.left() else throw err
                    else -> throw err
                }
                is SubjectMaxRevision -> when (rhs) {
                    is SubjectMinRevision -> if (lhs.subject == rhs.subject) {
                        SubjectRevisionRange(lhs.subject, rhs.revision, lhs.revision)
                            .mapLeft { conflict }
                    } else {
                        throw err
                    }
                    is SubjectMaxRevision -> if (lhs.subject == rhs.subject) conflict.left() else throw err
                    is SubjectRevisionRange -> if (lhs.subject == rhs.subject) conflict.left() else throw err
                    else -> throw err
                }
                is SubjectRevisionRange -> when (rhs) {
                    is SubjectMinRevision -> if (lhs.subject == rhs.subject) conflict.left() else throw err
                    is SubjectMaxRevision -> if (lhs.subject == rhs.subject) conflict.left() else throw err
                    is SubjectRevisionRange -> if (lhs.subject == rhs.subject) conflict.left() else throw err
                    else -> throw err
                }

                is SubjectMinRevisionOnStream -> when (rhs) {
                    is SubjectMaxRevisionOnStream ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject) {
                            SubjectStreamRevisionRange(
                                lhs.stream,
                                lhs.subject,
                                lhs.revision,
                                rhs.revision
                            ).mapLeft { conflict }
                        } else {
                            throw err
                        }
                    is SubjectMinRevisionOnStream ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject)
                            conflict.left()
                        else throw err
                    is SubjectStreamRevisionRange ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject)
                            conflict.left()
                        else throw err
                    else -> throw err
                }
                is SubjectMaxRevisionOnStream -> when (rhs) {
                    is SubjectMinRevisionOnStream ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject) {
                            SubjectStreamRevisionRange(
                                lhs.stream,
                                lhs.subject,
                                rhs.revision,
                                lhs.revision
                            ).mapLeft{ conflict }
                        } else {
                            throw err
                        }
                    is SubjectMaxRevisionOnStream ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject)
                            conflict.left()
                        else throw err
                    is SubjectStreamRevisionRange ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject)
                            conflict.left()
                        else throw err
                    else -> throw err
                }
                is SubjectStreamRevisionRange -> when (rhs) {
                    is SubjectMinRevisionOnStream ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject)
                            conflict.left()
                        else throw err
                    is SubjectMaxRevisionOnStream ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject)
                            conflict.left()
                        else throw err
                    is SubjectStreamRevisionRange ->
                        if (lhs.stream == rhs.stream && lhs.subject == rhs.subject)
                            conflict.left()
                        else throw err
                    else -> throw err
                }
            }
        }
    }
}

interface Batch {
    val database: DatabaseName
    val basis: Revision
    val revision: Revision
    val timestamp: Instant
}

interface BatchDetail: Batch {
    val events: List<Event>
}
