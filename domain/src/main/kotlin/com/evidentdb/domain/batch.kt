package com.evidentdb.domain

import arrow.core.*
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