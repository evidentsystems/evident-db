package com.evidentdb.domain_model.command

import arrow.core.*
import arrow.core.raise.either
import com.evidentdb.application.base32HexStringToLong
import com.evidentdb.cloudevents.EventSequenceExtension
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.StreamState
import com.evidentdb.domain_model.valikate
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import org.valiktor.ConstraintViolationException
import org.valiktor.DefaultConstraintViolation
import org.valiktor.constraints.NotNull
import org.valiktor.functions.matches
import org.valiktor.validate
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.util.*

fun validateStreamName(name: String)
        : Either<InvalidStreamName, StreamName> =
    when (val streamName = StreamName.of(name)) {
        is Either.Left -> InvalidStreamName(name).left()
        is Either.Right -> streamName.value.right()
    }

// TODO: regex validation?
fun validateEventType(eventType: EventType)
        : Either<InvalidEventType, EventType> =
    if (eventType.isNotEmpty())
        eventType.right()
    else
        InvalidEventType(eventType).left()

fun validateEventSubject(subjectString: String?)
        : Either<InvalidEventSubject, EventSubject> =
    EventSubject.of(subjectString)

fun validateUnvalidatedProposedEvents(
    databaseName: DatabaseName,
    events: Iterable<UnvalidatedProposedEvent>
): Validated<InvalidBatchError, List<ProposedEvent>> {
    if (events.toList().isEmpty())
        return NoEventsProvidedError.invalid()
    val (errors, validatedEvents) = events
        .map { event ->
            event.validate(databaseName)
        }
        .separateValidated()
    return if (errors.isEmpty())
        validatedEvents.valid()
    else
        InvalidEventsError(errors).invalid()
}

fun validateStreamState(
    databaseName: DatabaseName,
    currentStreamState: StreamState,
    event: ProposedEvent
): Either<StreamStateConflict, Event> {
    val valid = Event(
        databaseName,
        event.stream,
        event.event
    ).right()
    val invalid = StreamStateConflict(
        event,
        currentStreamState
    ).left()
    return when (event.streamState) {
        is ProposedEventStreamConstraint.NoStream ->
            when (currentStreamState) {
                is StreamState.NoStream -> valid
                else -> invalid
            }

        is ProposedEventStreamConstraint.StreamExists ->
            when (currentStreamState) {
                is StreamState.AtRevision -> valid
                else -> invalid
            }

        is ProposedEventStreamConstraint.AtRevision ->
            when (currentStreamState) {
                is StreamState.AtRevision -> {
                    if (currentStreamState.revision == event.streamState.revision)
                        valid
                    else
                        invalid
                }

                else -> invalid
            }

        is ProposedEventStreamConstraint.Any -> valid
        is ProposedEventStreamConstraint.NoSubject -> TODO()
        is ProposedEventStreamConstraint.SubjectAtRevision -> TODO()
        is ProposedEventStreamConstraint.SubjectExists -> TODO()
    }
}

suspend fun validateProposedEvent(
    database: ActiveDatabaseCommandModel,
    event: ProposedEvent,
    index: Int,
    timestamp: Instant
): Either<StreamStateConflict, Event> =
    either {
        val validEvent = validateStreamState(
            database.name,
            streamStateFromRevisions(database.clock, event.stream),
            event
        ).bind()
        validEvent.copy(
            event =
            CloudEventBuilder.v1(validEvent.event)
                .withExtension(
                    EventSequenceExtension(
                        database.revision + index + 1
                    )
                )
                .withTime(timestamp.atOffset(ZoneOffset.UTC))
                .build()
        )
    }

// Events

typealias EventId = UUID
typealias EventIndex = Long
typealias EventKey = String
typealias EventType = String

@JvmInline
value class EventSubject private constructor(val value: String) {
    companion object {
        fun build(value: String?): EventSubject =
            if (value == null) {
                throw ConstraintViolationException(setOf(DefaultConstraintViolation("value", constraint = NotNull)))
            } else {
                validate(EventSubject(value)) {
                    validate(EventSubject::value).matches(Regex(NAME_PATTERN))
                }
            }

        fun of(value: String?): Either<InvalidEventSubject, EventSubject> =
            valikate { build(value) }.mapLeft { InvalidEventSubject(value) }
    }
}

// TODO: add tenant
fun eventSource(
    databaseName: DatabaseName,
    event: UnvalidatedProposedEvent
): URI =
    URI("$DB_URI_SCHEME://${databaseName.value}/${event.stream}")

sealed interface ProposedEventStreamConstraint {
    object Any : ProposedEventStreamConstraint
    object StreamExists : ProposedEventStreamConstraint
    object NoStream : ProposedEventStreamConstraint
    data class AtRevision(val revision: StreamRevision) : ProposedEventStreamConstraint

    data class SubjectExists(val subject: EventSubject) : ProposedEventStreamConstraint
    data class NoSubject(val subject: EventSubject) : ProposedEventStreamConstraint
    data class SubjectAtRevision(val subject: EventSubject, val revision: StreamRevision) :
        ProposedEventStreamConstraint
}

data class UnvalidatedProposedEvent(
    val event: CloudEvent,
    val stream: String,
    val streamState: ProposedEventStreamConstraint = ProposedEventStreamConstraint.Any,
) {
    fun validate(
        databaseName: DatabaseName,
    ): Either<InvalidEvent, ProposedEvent> = either {
        validateStreamName(stream).zip(
            Semigroup.nonEmptyList(),
            validateEventType(event.type),
            validateEventSubject(event.subject)
        ) { streamName, _, _ ->
            val newEvent = CloudEventBuilder
                .from(this.event)
                .withSource(eventSource(databaseName, this))
                .build()
            ProposedEvent(
                newEvent,
                streamName,
                this.streamState,
            )
        }.mapLeft { InvalidEvent(this, it) }
    }
}

data class ProposedEvent(
    val event: CloudEvent,
    val stream: StreamName,
    val streamState: ProposedEventStreamConstraint = ProposedEventStreamConstraint.Any,
)

data class Event(
    val database: DatabaseName,
    val stream: StreamName,
    val event: CloudEvent
) {
    val index: EventIndex
        get() = event.getExtension("sequence").let {
            when (it) {
                is String -> base32HexStringToLong(it)
                else -> throw IllegalStateException("Event Sequence must be a String")
            }
        }
    val id: EventId
        get() = UUID.fromString(event.id)
}

typealias BatchId = UUID
typealias BatchKey = String

data class ProposedBatch(
    val id: BatchId,
    val databaseName: DatabaseName,
    val events: List<ProposedEvent>
) {
    suspend fun validate(
        database: ActiveDatabaseCommandModel,
    ): Either<BatchTransactionError, Batch> {
        if (batchRepository.batch(database.name, this.id).isRight()) {
            return DuplicateBatchError(this).left()
        }
        val timestamp = Instant.now()
        val (errors, events) = this.events.mapIndexed { index, proposedEvent ->
            validateProposedEvent(database, proposedEvent, index, timestamp)
        }.separateEither()
        return if (errors.isEmpty())
            Batch(
                this.id,
                database.name,
                events,
                timestamp,
                database.revision,
            ).right()
        else
            StreamStateConflictsError(errors).left()
    }
}

data class Batch(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<Event>,
    val timestamp: Instant,
    val revisionBefore: DatabaseRevision,
) {
    val revisionAfter: DatabaseRevision
        get() = revisionBefore + events.size
}

// Errors

sealed interface InvalidBatchError : BatchTransactionError

object NoEventsProvidedError : InvalidBatchError

data class InvalidStreamName(val streamName: String) : EventInvalidation
data class InvalidEventSubject(val eventSubject: String?) : EventInvalidation
data class InvalidEventType(val eventType: EventType) : EventInvalidation

data class InvalidEvent(val event: UnvalidatedProposedEvent, val errors: List<EventInvalidation>)
data class InvalidEventsError(val invalidEvents: List<InvalidEvent>) : InvalidBatchError

data class DuplicateBatchError(val batch: ProposedBatch) : BatchTransactionError
data class StreamStateConflict(val event: ProposedEvent, val streamState: StreamState)
data class StreamStateConflictsError(val conflicts: List<StreamStateConflict>) : BatchTransactionError
