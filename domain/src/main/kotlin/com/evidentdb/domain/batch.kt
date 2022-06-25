package com.evidentdb.domain

import arrow.core.*
import arrow.core.computations.either
import arrow.typeclasses.Semigroup
import java.net.URI

const val BATCH_URI_PATH_PREFIX = "/batches/"

fun buildBatchKey(databaseId: DatabaseId, batchId: BatchId): String =
    URI(
        "evdb",
        databaseId.toString(),
        "${BATCH_URI_PATH_PREFIX}${batchId}",
        null
    ).toString()


fun parseBatchKey(batchKey: BatchKey) : Pair<DatabaseId, BatchId> {
    val uri = URI(batchKey)
    return Pair(
        DatabaseId.fromString(uri.host),
        BatchId.fromString(uri.path.substring(BATCH_URI_PATH_PREFIX.length))
    )
}

// TODO: regex validation?
fun validateEventType(eventType: EventType)
        : ValidatedNel<InvalidEventType, EventType> =
    if (eventType.isNotEmpty())
        eventType.validNel()
    else
        InvalidEventType(eventType).invalidNel()

// TODO: regex validation?
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

fun validateUnvalidatedProposedEvent(event: UnvalidatedProposedEvent)
        : Validated<InvalidEventError, ProposedEvent> =
    validateStreamName(event.stream).zip(
        Semigroup.nonEmptyList(),
        validateEventType(event.type),
        validateEventAttributes(event.attributes)
    ) { _, _, _ ->
        ProposedEvent(
            EventId.randomUUID(),
            event.type,
            event.stream,
            event.streamState,
            event.data,
            event.attributes
        )
    }.mapLeft { InvalidEventError(event, it) }

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
    databaseId: DatabaseId,
    currentStreamState: StreamState,
    event: ProposedEvent
): Validated<StreamStateConflictError, Event> {
    val valid = Event(
        event.id,
        event.type,
        event.attributes,
        event.data,
        databaseId,
        event.stream
    ).valid()
    val invalid = StreamStateConflictError(event).invalid()
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
    databaseId: DatabaseId,
    streamReadModel: StreamReadModel,
    event: ProposedEvent
): Either<StreamStateConflictError, Event> =
    either {
        val validEvent = validateStreamState(
            databaseId,
            streamReadModel.streamState(databaseId, event.stream),
            event
        ).bind()
        // TODO: add to other index stream(s)
        validEvent
    }

suspend fun validateProposedBatch(
    databaseId: DatabaseId,
    streamReadModel: StreamReadModel,
    batch: ProposedBatch
): Either<BatchTransactionError, Batch> {
    val (errors, events) = batch.events.map{
        validateProposedEvent(databaseId, streamReadModel, it)
    }.separateEither()
    return if (errors.isEmpty())
            Batch(batch.id, databaseId, events).right()
    else
        StreamStateConflictsError(errors).left()
}