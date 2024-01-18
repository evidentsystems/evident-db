package com.evidentdb.client

import arrow.core.NonEmptyList
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
    data class StreamExists(val stream: StreamName) : BatchConstraint
    data class StreamDoesNotExist(val stream: StreamName) : BatchConstraint
    data class StreamMaxRevision(val stream: StreamName, val revision: StreamRevision) : BatchConstraint

    data class SubjectExists(val subject: EventSubject) : BatchConstraint
    data class SubjectDoesNotExist(val subject: EventSubject) : BatchConstraint
    data class SubjectMaxRevision(
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint

    data class SubjectExistsOnStream(val stream: StreamName, val subject: EventSubject) : BatchConstraint
    data class SubjectDoesNotExistOnStream(val stream: StreamName, val subject: EventSubject) : BatchConstraint
    data class SubjectMaxRevisionOnStream(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: StreamRevision,
    ) : BatchConstraint
}

data class Batch(
    val database: DatabaseName,
    val events: NonEmptyList<Event>,
    val timestamp: Instant,
    val basisRevision: DatabaseRevision,
) {
    val revision: DatabaseRevision
        get() = basisRevision + events.size.toUInt()
}

data class BatchProposal(
    val events: NonEmptyList<CloudEvent>,
    val constraints: List<BatchConstraint>,
)
