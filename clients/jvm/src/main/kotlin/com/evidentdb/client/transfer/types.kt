package com.evidentdb.client.transfer

import arrow.core.toNonEmptyListOrNull
import com.evidentdb.client.*
import com.evidentdb.dto.v1.proto.BatchConstraint.ConstraintCase.*
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toDomain
import io.cloudevents.protobuf.toTransfer
import java.time.Instant
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.evidentdb.dto.v1.proto.BatchConstraint as ProtoBatchConstraint
import com.evidentdb.dto.v1.proto.Database as ProtoDatabase
import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent

fun Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

//fun Instant.toTimestamp(): Timestamp =
//    Timestamp.newBuilder()
//        .setSeconds(epochSecond)
//        .setNanos(nano)
//        .build()

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

//fun Database.toTransfer(): ProtoDatabase = ProtoDatabase.newBuilder()
//    .setName(name)
//    .setCreated(created.toTimestamp())
//    .setRevision(revision.toLong())
//    .build()
//
//fun Batch.toTransfer(): ProtoBatch = ProtoBatch.newBuilder()
//    .setDatabase(database)
//    .addAllEventRevisions(eventRevisions.map { it.toLong() })
//    .setTimestamp(timestamp.toTimestamp())
//    .setBasisRevision(revision.toLong())
//    .build()

fun Event.toTransfer(): ProtoCloudEvent = event.toTransfer()

fun BatchConstraint.toTransfer(): ProtoBatchConstraint {
    val builder = ProtoBatchConstraint.newBuilder()
    when (this) {
        is BatchConstraint.StreamExists ->
            builder.streamExistsBuilder.stream = stream
        is BatchConstraint.StreamDoesNotExist ->
            builder.streamDoesNotExistBuilder.stream = stream
        is BatchConstraint.StreamMaxRevision -> {
            builder.streamMaxRevisionBuilder.stream = stream
            builder.streamMaxRevisionBuilder.revision = revision.toLong()
        }
        is BatchConstraint.SubjectExists ->
            builder.subjectExistsBuilder.subject = subject
        is BatchConstraint.SubjectDoesNotExist ->
            builder.subjectDoesNotExistBuilder.subject = subject
        is BatchConstraint.SubjectMaxRevision -> {
            builder.subjectMaxRevisionBuilder.subject = subject
            builder.subjectMaxRevisionBuilder.revision = revision.toLong()
        }
        is BatchConstraint.SubjectExistsOnStream -> {
            builder.subjectExistsOnStreamBuilder.stream = stream
            builder.subjectExistsOnStreamBuilder.subject = subject
        }
        is BatchConstraint.SubjectDoesNotExistOnStream -> {
            builder.subjectDoesNotExistOnStreamBuilder.stream = stream
            builder.subjectDoesNotExistOnStreamBuilder.subject = subject
        }
        is BatchConstraint.SubjectMaxRevisionOnStream -> {
            builder.subjectMaxRevisionOnStreamBuilder.stream = stream
            builder.subjectMaxRevisionOnStreamBuilder.subject = subject
            builder.subjectMaxRevisionOnStreamBuilder.revision = revision.toLong()
        }
    }
    return builder.build()
}

fun ProtoBatchConstraint.toDomain(): BatchConstraint =
        when (constraintCase) {
            STREAM_EXISTS -> BatchConstraint.StreamExists(streamExists.stream)
            STREAM_DOES_NOT_EXIST -> BatchConstraint.StreamDoesNotExist(streamDoesNotExist.stream)
            STREAM_MAX_REVISION -> BatchConstraint.StreamMaxRevision(
                streamMaxRevision.stream,
                streamMaxRevision.revision.toULong()
            )
            SUBJECT_EXISTS -> BatchConstraint.SubjectExists(subjectExists.subject)
            SUBJECT_DOES_NOT_EXIST -> BatchConstraint.SubjectDoesNotExist(subjectDoesNotExist.subject)
            SUBJECT_MAX_REVISION ->  BatchConstraint.SubjectMaxRevision(
                subjectMaxRevision.subject,
                subjectMaxRevision.revision.toULong()
            )
            SUBJECT_EXISTS_ON_STREAM -> BatchConstraint.SubjectExistsOnStream(
                subjectExistsOnStream.stream,
                subjectExistsOnStream.subject
            )
            SUBJECT_DOES_NOT_EXIST_ON_STREAM -> BatchConstraint.SubjectDoesNotExistOnStream(
                subjectDoesNotExistOnStream.stream,
                subjectDoesNotExistOnStream.subject
            )
            SUBJECT_MAX_REVISION_ON_STREAM -> BatchConstraint.SubjectMaxRevisionOnStream(
                subjectMaxRevisionOnStream.stream,
                subjectMaxRevisionOnStream.subject,
                subjectMaxRevisionOnStream.revision.toULong(),
            )
            CONSTRAINT_NOT_SET, null -> throw IllegalArgumentException("constraintCase enum cannot be null")
        }

// Errors
// TODO: toDomain() impls for error details

