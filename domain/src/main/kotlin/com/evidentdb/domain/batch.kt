package com.evidentdb.domain

import arrow.core.*
import arrow.core.computations.either
import arrow.typeclasses.Semigroup
import io.cloudevents.core.builder.CloudEventBuilder
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
    databaseId: DatabaseId,
    currentStreamState: StreamState,
    event: ProposedEvent
): Validated<StreamStateConflict, Event> {
    val valid = Event(
        event.id,
        databaseId,
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
    databaseId: DatabaseId,
    streamReadModel: StreamReadModel,
    event: ProposedEvent
): Either<StreamStateConflict, Event> =
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