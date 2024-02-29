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

typealias Revision = ULong

// Database

typealias DatabaseName = String
typealias DatabaseRevision = Revision

data class Database(
    val name: DatabaseName,
    val revision: DatabaseRevision,
)

// Streams

typealias StreamName = String
typealias StreamRevision = Revision
typealias StreamSubject = String

// Events

typealias EventId = String
typealias EventType = String
typealias EventSubject = String
typealias EventRevision = Revision

data class Event(val event: CloudEvent) {
    val database: DatabaseName
        get() = event.source.path.split('/')[1]
    val stream: StreamName
        get() = event.source.path.split('/').last()
    val id: EventId
        get() = event.id
    val type: EventType
        get() = event.type
    val subject: EventSubject?
        get() = event.subject
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
    data class DatabaseMinRevision(val revision: DatabaseRevision) : BatchConstraint
    data class DatabaseMaxRevision(val revision: DatabaseRevision) : BatchConstraint
    data class DatabaseRevisionRange private constructor(
        val min: DatabaseRevision,
        val max: DatabaseRevision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                min: DatabaseRevision,
                max: DatabaseRevision,
            ): Either<BatchConstraintInvalidation, DatabaseRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                DatabaseRevisionRange(min, max)
            }
        }
    }

    data class StreamMinRevision(
        val stream: StreamName,
        val revision: StreamRevision
    ) : BatchConstraint
    data class StreamMaxRevision(
        val stream: StreamName,
        val revision: StreamRevision
    ) : BatchConstraint
    data class StreamRevisionRange private constructor(
        val stream: StreamName,
        val min: DatabaseRevision,
        val max: DatabaseRevision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                stream: StreamName,
                min: DatabaseRevision,
                max: DatabaseRevision,
            ): Either<BatchConstraintInvalidation, StreamRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                StreamRevisionRange(stream, min, max)
            }
        }
    }

    data class SubjectMinRevision(
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint
    data class SubjectMaxRevision(
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint
    data class SubjectRevisionRange private constructor(
        val subject: EventSubject,
        val min: DatabaseRevision,
        val max: DatabaseRevision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                subject: EventSubject,
                min: DatabaseRevision,
                max: DatabaseRevision,
            ): Either<BatchConstraintInvalidation, SubjectRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                SubjectRevisionRange(subject, min, max)
            }
        }
    }

    data class SubjectMinRevisionOnStream(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint
    data class SubjectMaxRevisionOnStream(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint
    data class SubjectStreamRevisionRange private constructor(
        val stream: StreamName,
        val subject: EventSubject,
        val min: DatabaseRevision,
        val max: DatabaseRevision,
    ): BatchConstraint {
        companion object {
            operator fun invoke(
                stream: StreamName,
                subject: EventSubject,
                min: DatabaseRevision,
                max: DatabaseRevision,
            ): Either<BatchConstraintInvalidation, SubjectStreamRevisionRange> = either {
                ensure(min <= max) { InvalidBatchConstraintRange(min, max) }
                SubjectStreamRevisionRange(stream, subject, min, max)
            }
        }
    }

    companion object {
        fun streamExists(stream: StreamName) = StreamMinRevision(stream, 1uL)

        fun streamDoesNotExist(stream: StreamName) = StreamMaxRevision(stream, 0uL)

        fun streamAtRevision(stream: StreamName, revision: StreamRevision) =
            StreamRevisionRange(stream, revision, revision)

        fun subjectExists(subject: EventSubject) = SubjectMinRevision(subject, 1uL)

        fun subjectDoesNotExist(subject: EventSubject) = SubjectMaxRevision(subject, 0uL)

        fun subjectAtRevision(subject: EventSubject, revision: StreamRevision) =
            SubjectRevisionRange(subject, revision, revision)

        fun subjectExistsOnStream(stream: StreamName, subject: EventSubject) =
            SubjectMinRevisionOnStream(stream, subject, 1uL)

        fun subjectDoesNotExistOnStream(stream: StreamName, subject: EventSubject) =
            SubjectMaxRevisionOnStream(stream, subject, 0uL)

        fun subjectAtRevisionOnStream(stream: StreamName, subject: EventSubject, revision: StreamRevision) =
            SubjectStreamRevisionRange(stream, subject, revision, revision)
    }
}

data class Batch(
    val database: DatabaseName,
    val basis: DatabaseRevision,
    val events: NonEmptyList<Event>,
    val timestamp: Instant,
) {
    val revision: DatabaseRevision
        get() = basis + events.size.toUInt()
}

data class BatchProposal(
    val events: NonEmptyList<CloudEvent>,
    val constraints: List<BatchConstraint>,
)
