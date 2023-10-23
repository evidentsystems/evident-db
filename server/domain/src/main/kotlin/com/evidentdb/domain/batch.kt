package com.evidentdb.domain

import arrow.core.*
import arrow.core.continuations.either
import arrow.typeclasses.Semigroup
import com.evidentdb.cloudevents.EventSequenceExtension
import io.cloudevents.core.builder.CloudEventBuilder
import java.time.Instant
import java.time.ZoneOffset

fun validateStreamName(name: String)
        : ValidatedNel<InvalidStreamName, StreamName> =
    when (val streamName = StreamName.of(name)) {
        is Validated.Valid -> streamName.value.validNel()
        is Validated.Invalid -> InvalidStreamName(name).invalidNel()
    }

// TODO: regex validation?
fun validateEventType(eventType: EventType)
        : ValidatedNel<InvalidEventType, EventType> =
    if (eventType.isNotEmpty())
        eventType.validNel()
    else
        InvalidEventType(eventType).invalidNel()

fun validateEventSubject(subjectString: String?)
        : ValidatedNel<InvalidEventSubject, EventSubject> =
    when (val eventSubject = EventSubject.of(subjectString)) {
        is Validated.Valid -> eventSubject.value.validNel()
        is Validated.Invalid -> InvalidEventSubject(subjectString!!).invalidNel()
    }

fun validateUnvalidatedProposedEvent(
    databaseName: DatabaseName,
    event: UnvalidatedProposedEvent
): Validated<InvalidEvent, ProposedEvent> =
    validateStreamName(event.stream).zip(
        Semigroup.nonEmptyList(),
        validateEventType(event.event.type),
        validateEventSubject(event.event.subject)
    ) { streamName, _, _ ->
        val newEvent = CloudEventBuilder
            .from(event.event)
            .withSource(eventSource(databaseName, event))
            .build()
        ProposedEvent(
            newEvent,
            streamName,
            event.streamState,
        )
    }.mapLeft { InvalidEvent(event, it) }

fun validateUnvalidatedProposedEvents(
    databaseName: DatabaseName,
    events: Iterable<UnvalidatedProposedEvent>
) : Validated<InvalidBatchError, List<ProposedEvent>> {
    if (events.toList().isEmpty())
        return NoEventsProvidedError.invalid()
    val (errors, validatedEvents) = events
        .map { event ->
            validateUnvalidatedProposedEvent(databaseName, event)
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
): Validated<StreamStateConflict, Event> {
    val valid = Event(
        databaseName,
        event.event,
        event.stream
    ).valid()
    val invalid = StreamStateConflict(
        event,
        currentStreamState
    ).invalid()
    return when (event.streamState) {
        is StreamState.NoStream ->
            when(currentStreamState) {
                is StreamState.NoStream -> valid
                else -> invalid
            }
        is StreamState.StreamExists ->
            when(currentStreamState) {
                is StreamState.AtRevision -> valid
                else -> invalid
            }
        is StreamState.AtRevision ->
            when(currentStreamState) {
                is StreamState.AtRevision -> {
                    if (currentStreamState.revision == event.streamState.revision)
                        valid
                    else
                        invalid
                }
                else -> invalid
            }
        is StreamState.Any -> valid
    }
}

suspend fun validateProposedEvent(
    database: Database,
    event: ProposedEvent,
    index: Int,
    timestamp: Instant
): Either<StreamStateConflict, Event> =
    either {
        val validEvent = validateStreamState(
            database.name,
            streamStateFromRevisions(database.streamRevisions, event.stream),
            event
        ).bind()
        validEvent.copy(event =
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

suspend fun validateProposedBatch(
    database: Database,
    batchSummaryReadModel: BatchSummaryReadModel,
    batch: ProposedBatch
): Either<BatchTransactionError, Batch> {
    batchSummaryReadModel.batchSummary(database.name, batch.id)?.let {
        return DuplicateBatchError(batch).left()
    }
    val timestamp = Instant.now()
    val (errors, events) = batch.events.mapIndexed { index, proposedEvent ->
        validateProposedEvent(database, proposedEvent, index, timestamp)
    }.separateEither()
    return if (errors.isEmpty())
        Batch(
            batch.id,
            database.name,
            events,
            nextStreamRevisions(events, database.streamRevisions),
            timestamp,
        ).right()
    else
        StreamStateConflictsError(errors).left()
}

fun nextStreamRevisions(
    events: List<Event>,
    initialStreamRevisions: Map<StreamName, StreamRevision>,
): Map<StreamName, StreamRevision> {
    val ret = initialStreamRevisions.toMutableMap()
    return events.fold(ret) { acc, event ->
        val revision = acc[event.stream] ?: 0
        acc[event.stream] = revision + 1
        acc
    }
}