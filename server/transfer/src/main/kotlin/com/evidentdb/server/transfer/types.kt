package com.evidentdb.server.transfer

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.nonEmptyListOf
import arrow.core.raise.either
import arrow.core.raise.zipOrAccumulate
import com.evidentdb.v1.proto.domain.BatchConstraint.ConstraintCase.*
import com.evidentdb.server.domain_model.*
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toTransfer
import java.time.Instant
import com.evidentdb.v1.proto.domain.Batch as ProtoBatch
import com.evidentdb.v1.proto.domain.BatchConstraint as ProtoBatchConstraint
import com.evidentdb.v1.proto.domain.BatchSummary as ProtoBatchSummary
import com.evidentdb.v1.proto.domain.Database as ProtoDatabase
import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent

fun Instant.toTimestamp(): Timestamp =
    Timestamp.newBuilder()
        .setSeconds(epochSecond)
        .setNanos(nano)
        .build()

fun Database.toTransfer(): ProtoDatabase = ProtoDatabase.newBuilder()
    .setName(name.value)
    .setRevision(revision.toLong())
    .build()

// For returning AcceptedBatch from transaction
fun BatchDetail.toTransfer(): ProtoBatch = ProtoBatch.newBuilder()
    .setDatabase(database.value)
    .setBasis(basis.toLong())
    .addAllEvents(events.map { it.event.toTransfer() })
    .setTimestamp(timestamp.toTimestamp())
    .build()

// For fetching BatchSummary for log, etc.
fun Batch.toTransfer(): ProtoBatchSummary = ProtoBatchSummary.newBuilder()
    .setDatabase(database.value)
    .setBasis(basis.toLong())
    .setRevision(revision.toLong())
    .setTimestamp(timestamp.toTimestamp())
    .build()

fun Event.toTransfer(): ProtoCloudEvent = event.toTransfer()

fun BatchConstraint.toTransfer(): ProtoBatchConstraint {
    val builder = ProtoBatchConstraint.newBuilder()
    when (this) {
        is BatchConstraint.DatabaseMinRevision ->
            builder.databaseMinRevisionBuilder.revision = this.revision.toLong()
        is BatchConstraint.DatabaseMaxRevision ->
            builder.databaseMaxRevisionBuilder.revision = this.revision.toLong()
        is BatchConstraint.DatabaseRevisionRange -> {
            builder.databaseRevisionRangeBuilder.min = this.min.toLong()
            builder.databaseRevisionRangeBuilder.max = this.max.toLong()
        }

        is BatchConstraint.StreamMinRevision -> {
            builder.streamMinRevisionBuilder.stream = this.stream.value
            builder.streamMinRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.StreamMaxRevision -> {
            builder.streamMaxRevisionBuilder.stream = this.stream.value
            builder.streamMaxRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.StreamRevisionRange -> {
            builder.streamRevisionRangeBuilder.stream = this.stream.value
            builder.streamRevisionRangeBuilder.min = this.min.toLong()
            builder.streamRevisionRangeBuilder.max = this.max.toLong()
        }

        is BatchConstraint.SubjectMinRevision -> {
            builder.subjectMinRevisionBuilder.subject = this.subject.value
            builder.subjectMinRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectMaxRevision -> {
            builder.subjectMaxRevisionBuilder.subject = this.subject.value
            builder.subjectMaxRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectRevisionRange -> {
            builder.subjectRevisionRangeBuilder.subject = this.subject.value
            builder.subjectRevisionRangeBuilder.min = this.min.toLong()
            builder.subjectRevisionRangeBuilder.max = this.max.toLong()
        }

        is BatchConstraint.SubjectMinRevisionOnStream -> {
            builder.subjectMinRevisionOnStreamBuilder.stream = this.stream.value
            builder.subjectMinRevisionOnStreamBuilder.subject = this.subject.value
            builder.subjectMinRevisionOnStreamBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectMaxRevisionOnStream -> {
            builder.subjectMaxRevisionOnStreamBuilder.stream = this.stream.value
            builder.subjectMaxRevisionOnStreamBuilder.subject = this.subject.value
            builder.subjectMaxRevisionOnStreamBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectStreamRevisionRange -> {
            builder.subjectStreamRevisionRangeBuilder.stream = this.stream.value
            builder.subjectStreamRevisionRangeBuilder.subject = this.subject.value
            builder.subjectStreamRevisionRangeBuilder.min = this.min.toLong()
            builder.subjectStreamRevisionRangeBuilder.max = this.max.toLong()
        }
    }
    return builder.build()
}

fun ProtoBatchConstraint.toDomain(): Either<NonEmptyList<BatchConstraintInvalidation>, BatchConstraint> =
    either {
        val proto = this@toDomain
        when (proto.constraintCase) {
            DATABASE_MIN_REVISION ->
                BatchConstraint.DatabaseMinRevision(proto.databaseMinRevision.revision.toULong())
            DATABASE_MAX_REVISION ->
                BatchConstraint.DatabaseMaxRevision(proto.databaseMaxRevision.revision.toULong())
            DATABASE_REVISION_RANGE ->
                BatchConstraint.DatabaseRevisionRange(
                    proto.databaseRevisionRange.min.toULong(),
                    proto.databaseRevisionRange.max.toULong()
                ).mapLeft { nonEmptyListOf(it) }.bind()

            STREAM_MIN_REVISION -> {
                val streamName = StreamName(proto.streamMinRevision.stream)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.StreamMinRevision(
                    streamName,
                    proto.streamMinRevision.revision.toULong()
                )
            }
            STREAM_MAX_REVISION -> {
                val streamName = StreamName(proto.streamMaxRevision.stream)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.StreamMaxRevision(
                    streamName,
                    proto.streamMaxRevision.revision.toULong()
                )
            }
            STREAM_REVISION_RANGE -> either {
                val streamName = StreamName(proto.streamRevisionRange.stream).bind()

                BatchConstraint.StreamRevisionRange(
                    streamName,
                    proto.streamRevisionRange.min.toULong(),
                    proto.streamRevisionRange.max.toULong(),
                ).bind()
            }.mapLeft { nonEmptyListOf(it) }.bind()

            SUBJECT_MIN_REVISION -> {
                val subjectName = EventSubject(proto.subjectMinRevision.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectMinRevision(
                    subjectName,
                    proto.subjectMinRevision.revision.toULong()
                )
            }
            SUBJECT_MAX_REVISION -> {
                val subjectName = EventSubject(proto.subjectMaxRevision.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectMaxRevision(
                    subjectName,
                    proto.subjectMaxRevision.revision.toULong()
                )
            }
            SUBJECT_REVISION_RANGE -> either {
                val subjectName = EventSubject(proto.subjectRevisionRange.subject).bind()
                BatchConstraint.SubjectRevisionRange(
                    subjectName,
                    proto.subjectRevisionRange.min.toULong(),
                    proto.subjectRevisionRange.max.toULong()
                ).bind()
            }.mapLeft { nonEmptyListOf(it) }.bind()

            SUBJECT_MIN_REVISION_ON_STREAM -> zipOrAccumulate(
                { StreamName(proto.subjectMinRevisionOnStream.stream).bind() },
                { EventSubject(proto.subjectMinRevisionOnStream.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectMinRevisionOnStream(
                    streamName,
                    subject,
                    proto.subjectMinRevisionOnStream.revision.toULong(),
                )
            }
            SUBJECT_MAX_REVISION_ON_STREAM -> zipOrAccumulate(
                { StreamName(proto.subjectMaxRevisionOnStream.stream).bind() },
                { EventSubject(proto.subjectMaxRevisionOnStream.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectMaxRevisionOnStream(
                    streamName,
                    subject,
                    proto.subjectMaxRevisionOnStream.revision.toULong(),
                )
            }
            SUBJECT_STREAM_REVISION_RANGE -> zipOrAccumulate(
                { StreamName(proto.subjectStreamRevisionRange.stream).bind() },
                { EventSubject(proto.subjectStreamRevisionRange.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectStreamRevisionRange(
                    streamName,
                    subject,
                    proto.subjectStreamRevisionRange.min.toULong(),
                    proto.subjectStreamRevisionRange.max.toULong(),
                ).mapLeft { nonEmptyListOf(it) }.bind()
            }

            CONSTRAINT_NOT_SET, null ->
                throw IllegalArgumentException("constraintCase enum cannot be null")
        }
    }
