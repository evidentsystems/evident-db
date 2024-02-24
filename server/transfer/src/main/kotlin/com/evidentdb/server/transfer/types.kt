package com.evidentdb.server.transfer

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.nonEmptyListOf
import arrow.core.raise.either
import arrow.core.raise.zipOrAccumulate
import com.evidentdb.dto.v1.proto.BatchConstraint.ConstraintCase.*
import com.evidentdb.server.domain_model.*
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toTransfer
import java.time.Instant
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.evidentdb.dto.v1.proto.BatchConstraint as ProtoBatchConstraint
import com.evidentdb.dto.v1.proto.BatchSummary as ProtoBatchSummary
import com.evidentdb.dto.v1.proto.Database as ProtoDatabase
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
fun IndexedBatch.toTransfer(): ProtoBatch = ProtoBatch.newBuilder()
    .setDatabase(database.value)
    .addAllEvents(events.map { it.event.toTransfer() })
    .setTimestamp(timestamp.toTimestamp())
    .setBasis(basis.toLong())
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
        is BatchConstraint.StreamExists ->
            builder.streamExistsBuilder.stream = stream.value
        is BatchConstraint.StreamDoesNotExist ->
            builder.streamDoesNotExistBuilder.stream = stream.value
        is BatchConstraint.StreamMaxRevision -> {
            builder.streamMaxRevisionBuilder.stream = stream.value
            builder.streamMaxRevisionBuilder.revision = revision.toLong()
        }
        is BatchConstraint.SubjectExists ->
            builder.subjectExistsBuilder.subject = subject.value
        is BatchConstraint.SubjectDoesNotExist ->
            builder.subjectDoesNotExistBuilder.subject = subject.value
        is BatchConstraint.SubjectMaxRevision -> {
            builder.subjectMaxRevisionBuilder.subject = subject.value
            builder.subjectMaxRevisionBuilder.revision = revision.toLong()
        }
        is BatchConstraint.SubjectExistsOnStream -> {
            builder.subjectExistsOnStreamBuilder.stream = stream.value
            builder.subjectExistsOnStreamBuilder.subject = subject.value
        }
        is BatchConstraint.SubjectDoesNotExistOnStream -> {
            builder.subjectDoesNotExistOnStreamBuilder.stream = stream.value
            builder.subjectDoesNotExistOnStreamBuilder.subject = subject.value
        }
        is BatchConstraint.SubjectMaxRevisionOnStream -> {
            builder.subjectMaxRevisionOnStreamBuilder.stream = stream.value
            builder.subjectMaxRevisionOnStreamBuilder.subject = subject.value
            builder.subjectMaxRevisionOnStreamBuilder.revision = revision.toLong()
        }
    }
    return builder.build()
}

fun ProtoBatchConstraint.toDomain(): Either<NonEmptyList<BatchConstraintInvalidation>, BatchConstraint> =
    either {
        val proto = this@toDomain
        when (proto.constraintCase) {
            STREAM_EXISTS -> {
                val streamName = StreamName(proto.streamExists.stream)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.StreamExists(streamName)
            }
            STREAM_DOES_NOT_EXIST -> {
                val streamName = StreamName(proto.streamDoesNotExist.stream)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.StreamDoesNotExist(streamName)
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
            SUBJECT_EXISTS -> {
                val subjectName = EventSubject(proto.subjectExists.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectExists(subjectName)
            }
            SUBJECT_DOES_NOT_EXIST -> {
                val subjectName = EventSubject(proto.subjectDoesNotExist.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectDoesNotExist(subjectName)
            }
            SUBJECT_MAX_REVISION ->  {
                val subjectName = EventSubject(proto.subjectMaxRevision.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectMaxRevision(
                    subjectName,
                    proto.subjectMaxRevision.revision.toULong()
                )
            }
            SUBJECT_EXISTS_ON_STREAM -> zipOrAccumulate(
                { StreamName(proto.subjectExistsOnStream.stream).bind() },
                { EventSubject(proto.subjectExistsOnStream.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectExistsOnStream(streamName, subject)
            }
            SUBJECT_DOES_NOT_EXIST_ON_STREAM -> zipOrAccumulate(
                { StreamName(proto.subjectDoesNotExistOnStream.stream).bind() },
                { EventSubject(proto.subjectDoesNotExistOnStream.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectDoesNotExistOnStream(streamName, subject)
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
            CONSTRAINT_NOT_SET -> raise(nonEmptyListOf(EmptyBatchConstraint))
            null -> throw IllegalArgumentException("constraintCase enum cannot be null")
        }
    }
