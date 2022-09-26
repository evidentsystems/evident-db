package com.evidentdb.domain

import arrow.core.*
import arrow.core.computations.either
import arrow.typeclasses.Semigroup
import io.cloudevents.core.builder.CloudEventBuilder

// TODO: regex validation?
fun validateEventType(eventType: EventType)
        : ValidatedNel<InvalidEventType, EventType> =
    if (eventType.isNotEmpty())
        eventType.validNel()
    else
        InvalidEventType(eventType).invalidNel()

fun validateUnvalidatedProposedEvent(event: UnvalidatedProposedEvent)
        : Validated<InvalidEvent, ProposedEvent> =
    validateStreamName(event.stream).zip(
        Semigroup.nonEmptyList(),
        validateEventType(event.event.type)
    ) { _, _ ->
        val id = EventId.randomUUID()
        val newEvent = CloudEventBuilder.from(event.event)
            .withId(id.toString())
            .build()
        ProposedEvent(
            id,
            newEvent,
            event.stream,
            event.streamState,
        )
    }.mapLeft { InvalidEvent(event, it) }

fun validateUnvalidatedProposedEvents(events: Iterable<UnvalidatedProposedEvent>)
        : Validated<InvalidBatchError, List<ProposedEvent>> {
    if (events.toList().isEmpty())
        return NoEventsProvidedError.invalid()
    val (errors, validatedEvents) = events
        .map(::validateUnvalidatedProposedEvent)
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
        event.id,
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
    databaseName: DatabaseName,
    streamReadModel: StreamReadModel,
    event: ProposedEvent
): Either<StreamStateConflict, Event> =
    either {
        val validEvent = validateStreamState(
            databaseName,
            streamReadModel.streamState(databaseName, event.stream),
            event
        ).bind()
        // TODO: add to other index stream(s)
        validEvent
    }

suspend fun validateProposedBatch(
    databaseName: DatabaseName,
    streamReadModel: StreamReadModel,
    batchReadModel: BatchReadModel,
    batch: ProposedBatch
): Either<BatchTransactionError, Batch> {
    batchReadModel.batch(batch.id)?.let {
        return DuplicateBatchError(batch).left()
    }
    val (errors, events) = batch.events.map{
        validateProposedEvent(databaseName, streamReadModel, it)
    }.separateEither()
    return if (errors.isEmpty())
        Batch(batch.id, databaseName, events).right()
    else
        StreamStateConflictsError(errors).left()
}