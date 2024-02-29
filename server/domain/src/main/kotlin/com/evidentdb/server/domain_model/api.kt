package com.evidentdb.server.domain_model

import arrow.core.*
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import arrow.core.raise.zipOrAccumulate
import com.evidentdb.server.cloudevents.RecordedTimeExtension
import com.evidentdb.server.cloudevents.SequenceExtension
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import java.time.Instant
import java.time.ZoneOffset

sealed interface DatabaseCommandModel

// Adapter API

interface DatabaseCommandModelBeforeCreation: DatabaseCommandModel {
    // Invariant Checking
    suspend fun databaseNameAvailable(name: DatabaseName): Boolean

    // Command Processing
    suspend fun buildNewlyCreatedDatabase(
        name: DatabaseName
    ): Either<DatabaseCreationError, NewlyCreatedDatabaseCommandModel> = either {
        ensure(databaseNameAvailable(name)) { DatabaseNameAlreadyExists(name) }
        NewlyCreatedDatabaseCommandModel(name, this@DatabaseCommandModelBeforeCreation)
    }
}

interface CleanDatabaseCommandModel: ActiveDatabaseCommandModel

// Domain API

sealed interface ActiveDatabaseCommandModel: DatabaseCommandModel, Database {
    override val name: DatabaseName
    override val revision: DatabaseRevision

    // Command Processing
    fun buildAcceptedBatch(
        proposedBatch: WellFormedProposedBatch
    ): Either<BatchTransactionError, IndexedBatch> =
        proposedBatch.indexBatch(this.revision)

    // Event Transitions
    fun withBatch(batch: IndexedBatch): DirtyDatabaseCommandModel =
        DirtyDatabaseCommandModel(this@ActiveDatabaseCommandModel, batch)

    fun asDeleted(): DatabaseCommandModelAfterDeletion =
        DatabaseCommandModelAfterDeletion(this@ActiveDatabaseCommandModel)
}

data class NewlyCreatedDatabaseCommandModel internal constructor(
    override val name: DatabaseName,
    private val basis: DatabaseCommandModelBeforeCreation,
): ActiveDatabaseCommandModel {
    override val revision: DatabaseRevision = 0uL
}

data class DirtyDatabaseCommandModel internal constructor(
    val basis: ActiveDatabaseCommandModel,
    val batch: IndexedBatch,
) : ActiveDatabaseCommandModel {
    override val name: DatabaseName
        get() = basis.name
    override val revision: DatabaseRevision = basis.revision + batch.events.size.toUInt()

    // Recursively discovers last clean basis
    val dirtyRelativeToRevision: DatabaseRevision
        get() = if (basis is DirtyDatabaseCommandModel) {
            basis.dirtyRelativeToRevision
        } else {
            basis.revision
        }

    // Provides dirty batches in order
    val dirtyBatches: List<IndexedBatch>
        get() = if (basis is DirtyDatabaseCommandModel) {
            val result = basis.dirtyBatches.toMutableList()
            result.add(batch)
            result.toList()
        } else {
            listOf(batch)
        }
}

data class DatabaseCommandModelAfterDeletion internal constructor(
    private val basis: ActiveDatabaseCommandModel
): DatabaseCommandModel, Database by basis

// Batch Transaction Lifecycle

//// Event

/**
 *  Events begin as ProposedEvents. We don't initially know if this event is
 *  a) well-formed or b) has a unique source + ID globally and within its batch.
 *
 * */
data class ProposedEvent(val event: CloudEvent) {
    val uniqueKey = Pair(event.source.toString(), event.id)

    fun ensureWellFormed(batch: ProposedBatch): Either<InvalidEvent, WellFormedProposedEvent> =
        WellFormedProposedEvent(this, batch)
            .mapLeft { InvalidEvent(event, it) }
}

data class WellFormedProposedEvent private constructor (val event: CloudEvent) {
    val stream: StreamName
        get() = event.source.path.split('/').last().let { name ->
            StreamName(name).getOrElse {
                throw IllegalStateException("Illegal event source (stream name): $name")
            }
        }
    val id: EventId
        get() = EventId(event.id).getOrElse { throw IllegalStateException("Illegal event id: $event.id") }

    companion object {
        /**
         *  Given a proposed event, its proposed batch, and a base event source URI,
         *  return either a NEL of EventInvalidations, or a WellFormedProposedEvent.
         *
         *  The stream is provided by the client in the source attribute as a relative URI path,
         *  e.g. 'my-stream', which we'll transform to be relative to the fully qualified
         *  database URI.
         */
        operator fun invoke(
            proposed: ProposedEvent,
            batch: ProposedBatch,
        ): Either<NonEmptyList<EventInvalidation>, WellFormedProposedEvent> = either {
            zipOrAccumulate(
                {
                    val proposedEventSourceURI = ProposedEventSourceURI(proposed.event.source).bind()
                    val streamName = proposedEventSourceURI.toStreamName().bind()
                    batch.databasePathEventSourceURI.toEventSourceURI(streamName).bind()
                },
                { EventId(proposed.event.id).bind() },
                { EventType(proposed.event.type).bind() },
                { proposed.event.subject?.let { EventSubject(it).bind() } },
                {
                    ensure(batch.eventKeyIsUniqueInBatch(proposed)) {
                        DuplicateEventId(proposed.event.source.toString(), proposed.event.id)
                    }
                }
            ) { source, _, _, _, _ ->
                val newEvent = CloudEventBuilder
                    .from(proposed.event)
                    .withSource(source.value)
                    .build()
                WellFormedProposedEvent(newEvent)
            }
        }
    }

    fun index(
        basisRevision: DatabaseRevision,
        indexInBatch: UInt,
        timestamp: Instant
    ): Either<InvalidEvent, IndexedEvent>  =
        IndexedEvent(this, basisRevision, indexInBatch, timestamp)
            .mapLeft { InvalidEvent(event, it) }
}

data class IndexedEvent private constructor(override val event: CloudEvent): Event {
    companion object {
        // For lack of a better place, let's register our required CloudEvent extensions here
        init {
            SequenceExtension.register()
            RecordedTimeExtension.register()
        }

        operator fun invoke(
            wellFormed: WellFormedProposedEvent,
            basisRevision: DatabaseRevision,
            indexInBatch: UInt,
            timestamp: Instant
        ): Either<NonEmptyList<EventInvalidation>, IndexedEvent> {
            val sequenceExtension = SequenceExtension()
            sequenceExtension.sequence = basisRevision + indexInBatch + 1u
            val newEvent = CloudEventBuilder.from(wellFormed.event)
                .withExtension(sequenceExtension)
                .withTime(timestamp.atOffset(ZoneOffset.UTC))
                .build()
            return IndexedEvent(newEvent).right()
        }
    }
}

//// Batch

data class ProposedBatch(
    val databasePathEventSourceURI: DatabasePathEventSourceURI,
    val events: List<ProposedEvent>,
    val constraints: List<BatchConstraint>
) {
    private val nonUniqueKeys = events
        .groupingBy { it.uniqueKey }
        .eachCount()
        .filterValues { it > 1 }

    fun eventKeyIsUniqueInBatch(proposed: ProposedEvent) =
        !nonUniqueKeys.containsKey(proposed.uniqueKey)

    fun ensureWellFormed() =
        WellFormedProposedBatch(this)
}

fun validateConstraints(
    constraints: List<BatchConstraint>
): Either<BatchConstraintConflicts, Map<BatchConstraintKey, BatchConstraint>> = either {
    val result = constraints.fold(Pair(
        mutableMapOf<BatchConstraintKey, BatchConstraint>(),
        mutableSetOf<BatchConstraintConflict>()
    )) { acc, constraint ->
        val key = BatchConstraintKey.from(constraint)
        val currentConstraint = acc.first[key]
        val nextConstraint = BatchConstraint.combine(currentConstraint, constraint)
        if (nextConstraint.isRight()) {
            acc.first[key] = nextConstraint.getOrNull()!!
        } else {
            acc.second.add(nextConstraint.leftOrNull()!!)
        }
        acc
    }
    ensure(result.second.isEmpty()) {
        BatchConstraintConflicts(result.second.toNonEmptyListOrNull()!!)
    }
    result.first
}

data class WellFormedProposedBatch(
    val databaseName: DatabaseName,
    val events: NonEmptyList<WellFormedProposedEvent>,
    val constraints: Map<BatchConstraintKey, BatchConstraint>
) {
    companion object {
        operator fun invoke(proposedBatch: ProposedBatch): Either<BatchTransactionError, WellFormedProposedBatch> =
            either {
                val nonEmptyEvents = proposedBatch.events.toNonEmptyListOrNull()
                ensureNotNull(nonEmptyEvents) { EmptyBatch }

                val wellFormedConstraints = validateConstraints(proposedBatch.constraints).bind()

                val wellFormedEvents = nonEmptyEvents.mapOrAccumulate {
                    it.ensureWellFormed(proposedBatch).bind()
                }.mapLeft { InvalidEvents(it) }.bind()
                WellFormedProposedBatch(
                    proposedBatch.databasePathEventSourceURI.databaseName,
                    wellFormedEvents,
                    wellFormedConstraints
                )
            }
    }

    fun indexBatch(
        basisRevision: DatabaseRevision
    ) = IndexedBatch(this, basisRevision)
}

data class IndexedBatch private constructor(
    override val database: DatabaseName,
    val events: NonEmptyList<IndexedEvent>,
    override val timestamp: Instant,
    override val basis: DatabaseRevision,
    val constraints: Map<BatchConstraintKey, BatchConstraint>,
): Batch {
    override val revision
        get() = basis + events.size.toUInt()

    companion object {
        operator fun invoke(
            wellFormed: WellFormedProposedBatch,
            basisRevision: DatabaseRevision,
        ): Either<BatchTransactionError, IndexedBatch> = either {
            // Validate constituent events
            val timestamp = Instant.now()
            val events = wellFormed.events.withIndex()
                .mapOrAccumulate { eventWithIndex ->
                    eventWithIndex.value.index(
                        basisRevision,
                        eventWithIndex.index.toUInt(),
                        timestamp
                    ).bind()
                }.mapLeft { InvalidEvents(it) }.bind().toNonEmptyListOrNull()
            IndexedBatch(
                wellFormed.databaseName,
                events!!,
                timestamp,
                basisRevision,
                wellFormed.constraints,
            )
        }
    }
}
