package com.evidentdb.domain

import arrow.core.*
import arrow.core.computations.either
import arrow.typeclasses.Semigroup

fun validateStreamName(streamName: StreamName)
        : ValidatedNel<InvalidStreamName, StreamName> =
    if (streamName.isNotEmpty())
        streamName.validNel()
    else
        InvalidStreamName(streamName).invalidNel()

fun validateEventType(eventType: EventType)
        : ValidatedNel<InvalidEventType, EventType> =
    if (eventType.isNotEmpty())
        eventType.validNel()
    else
        InvalidEventType(eventType).invalidNel()

fun validateEventAttribute(attributeKey: EventAttributeKey)
        : ValidatedNel<InvalidEventAttribute, EventAttributeKey> =
    if (attributeKey.isNotEmpty())
        attributeKey.validNel()
    else
        InvalidEventAttribute(attributeKey).invalidNel()

fun validateEventAttributes(attributes: Map<EventAttributeKey, EventAttributeValue>)
        : ValidatedNel<InvalidEventAttribute, List<EventAttributeKey>> =
    attributes.keys.traverseValidated(
        Semigroup.nonEmptyList(),
        ::validateEventAttribute
    )

fun validateProposedEvent(event: UnvalidatedProposedEvent)
        : Validated<InvalidEventError, ProposedEvent> =
    validateStreamName(event.stream).zip(
        Semigroup.nonEmptyList(),
        validateEventType(event.type),
        validateEventAttributes(event.attributes)
    ) { _, _, _ ->
        ProposedEvent(
            EventId.randomUUID(),
            event.type,
            event.attributes,
            event.data,
            event.stream,
            event.streamState
        )
    }.mapLeft { InvalidEventError(event, it) }

fun validateProposedEvents(events: Iterable<UnvalidatedProposedEvent>)
        : Validated<InvalidEventsError, Iterable<ProposedEvent>> {
    val (errors, validatedEvents) = events.map(::validateProposedEvent).separateValidated()
    return if (errors.isEmpty())
        validatedEvents.valid()
    else
        InvalidEventsError(errors).invalid()
}

fun validateStreamState(
    streamState: StreamState,
    event: ProposedEvent
): Validated<StreamStateConflictError, Event> {
    val valid = Event(
        event.id,
        event.type,
        event.attributes,
        event.data
    ).valid()
    val invalid = StreamStateConflictError(event).invalid()
    return when (event.streamState) {
        is StreamState.NoStream ->
            when(streamState) {
                is StreamState.NoStream -> valid
                else -> invalid
            }
        is StreamState.StreamExists ->
            when(streamState) {
                is StreamState.AtRevision -> valid
                else -> invalid
            }
        is StreamState.AtRevision ->
            when(streamState) {
                is StreamState.AtRevision -> {
                    if (streamState.revision == event.streamState.revision)
                        valid
                    else
                        invalid
                }
                else -> invalid
            }
        is StreamState.Any -> valid
    }
}

// TODO: make this pure based on stream state
suspend fun transactEvent(
    databaseId: DatabaseId,
    eventStore: WritableEventStore,
    streamStore: WritableStreamStore,
    event: ProposedEvent
): Either<StreamStateConflictError, Event> =
    either {
        val validEvent = validateStreamState(
            streamStore.streamState(databaseId, event.stream),
            event
        ).bind()
        eventStore.put(validEvent)
        streamStore.append(databaseId, event.stream, validEvent.id)
        // TODO: add to other index stream(s)
        validEvent
    }

// TODO: make this pure based on stream state
suspend fun transactProposedBatch(
    databaseId: DatabaseId,
    eventStore: WritableEventStore,
    streamStore: WritableStreamStore,
    batch: ProposedBatch
): Either<BatchTransactionError, Batch> {
    val (errors, events) = batch.events.map{
        transactEvent(databaseId, eventStore, streamStore, it)
    }.separateEither()
    return if (errors.isEmpty())
        Batch(batch.id, databaseId, events).right()
    else
        StreamStateConflictsError(errors).left()
}