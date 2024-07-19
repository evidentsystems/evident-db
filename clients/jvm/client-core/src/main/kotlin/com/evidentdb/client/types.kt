package com.evidentdb.client

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.evidentdb.client.cloudevents.RecordedTimeExtension
import com.evidentdb.client.cloudevents.SequenceExtension
import io.cloudevents.CloudEvent
import io.cloudevents.core.provider.ExtensionProvider
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias Revision = ULong

// Database

typealias DatabaseName = String

data class Database(
    val name: DatabaseName,
    val revision: Revision,
)

// Streams

typealias StreamName = String
typealias StreamSubject = String

// Events

typealias EventId = String
typealias EventType = String
typealias EventSubject = String

data class Event(val event: CloudEvent): CloudEvent by event {
    val database: DatabaseName
        get() = event.source.path.split('/')[1]
    val stream: StreamName
        get() = event.source.path.split('/').last()
    val revision: Revision
        get() = ExtensionProvider.getInstance()
            .parseExtension(SequenceExtension::class.java, event)!!
            .sequence
    val recordedTime: OffsetDateTime
        get() = ExtensionProvider.getInstance()
            .parseExtension(RecordedTimeExtension::class.java, event)!!
            .recordedTime.atOffset(ZoneOffset.UTC)
}

// Batch

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
    }
}

data class BatchSummary(
    val database: DatabaseName,
    val basis: Revision,
    val revision: Revision,
    val timestamp: Instant,
)

data class Batch(
    val database: DatabaseName,
    val basis: Revision,
    val events: NonEmptyList<Event>,
    val timestamp: Instant,
) {
    val revision: Revision
        get() = basis + events.size.toUInt()
}
