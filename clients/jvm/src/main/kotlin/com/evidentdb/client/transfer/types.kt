package com.evidentdb.client.transfer

import arrow.core.Either
import arrow.core.Either.Companion.zipOrAccumulate
import arrow.core.NonEmptyList
import arrow.core.nonEmptyListOf
import arrow.core.raise.either
import arrow.core.toNonEmptyListOrNull
import com.evidentdb.client.*
import com.evidentdb.dto.v1.proto.BatchConstraint.ConstraintCase.*
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toDomain
import java.time.Instant
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.evidentdb.dto.v1.proto.BatchConstraint as ProtoBatchConstraint
import com.evidentdb.dto.v1.proto.Database as ProtoDatabase

fun Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

fun ProtoDatabase.toDomain() = Database(
    name,
    revision.toULong()
)

fun ProtoBatch.toDomain() = Batch(
    database,
    basis.toULong(),
    eventsList.map { Event(it.toDomain()) }.toNonEmptyListOrNull()!!,
    timestamp.toInstant()
)

//fun Event.toTransfer(): ProtoCloudEvent = event.toTransfer()

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
            builder.streamMinRevisionBuilder.stream = this.stream
            builder.streamMinRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.StreamMaxRevision -> {
            builder.streamMaxRevisionBuilder.stream = this.stream
            builder.streamMaxRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.StreamRevisionRange -> {
            builder.streamRevisionRangeBuilder.stream = this.stream
            builder.streamRevisionRangeBuilder.min = this.min.toLong()
            builder.streamRevisionRangeBuilder.max = this.max.toLong()
        }

        is BatchConstraint.SubjectMinRevision -> {
            builder.subjectMinRevisionBuilder.subject = this.subject
            builder.subjectMinRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectMaxRevision -> {
            builder.subjectMaxRevisionBuilder.subject = this.subject
            builder.subjectMaxRevisionBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectRevisionRange -> {
            builder.subjectRevisionRangeBuilder.subject = this.subject
            builder.subjectRevisionRangeBuilder.min = this.min.toLong()
            builder.subjectRevisionRangeBuilder.max = this.max.toLong()
        }

        is BatchConstraint.SubjectMinRevisionOnStream -> {
            builder.subjectMinRevisionOnStreamBuilder.stream = this.stream
            builder.subjectMinRevisionOnStreamBuilder.subject = this.subject
            builder.subjectMinRevisionOnStreamBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectMaxRevisionOnStream -> {
            builder.subjectMaxRevisionOnStreamBuilder.stream = this.stream
            builder.subjectMaxRevisionOnStreamBuilder.subject = this.subject
            builder.subjectMaxRevisionOnStreamBuilder.revision = this.revision.toLong()
        }
        is BatchConstraint.SubjectStreamRevisionRange -> {
            builder.subjectStreamRevisionRangeBuilder.stream = this.stream
            builder.subjectStreamRevisionRangeBuilder.subject = this.subject
            builder.subjectStreamRevisionRangeBuilder.min = this.min.toLong()
            builder.subjectStreamRevisionRangeBuilder.max = this.max.toLong()
        }
    }
    return builder.build()
}

// Errors
// TODO: toDomain() impls for error details

