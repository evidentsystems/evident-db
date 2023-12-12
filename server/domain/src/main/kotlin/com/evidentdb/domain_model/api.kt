package com.evidentdb.domain_model

import arrow.core.*
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import arrow.core.raise.zipOrAccumulate
import com.evidentdb.cloudevents.SequenceExtension
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import kotlinx.coroutines.runBlocking
import java.time.Instant
import java.time.ZoneOffset

sealed interface DatabaseCommandModel

// Adapter API

interface DatabaseCommandModelBeforeCreation: DatabaseCommandModel {
    suspend fun databaseNameAvailable(name: DatabaseName): Boolean

    suspend fun create(
        name: DatabaseName,
        subscriptionURI: DatabaseSubscriptionURI,
        created: Instant
    ): Either<DatabaseCreationError, NewlyCreatedDatabaseCommandModel> = either {
        ensure(databaseNameAvailable(name)) { DatabaseNameAlreadyExists(name) }
        NewlyCreatedDatabaseCommandModel(name, subscriptionURI, created, this@DatabaseCommandModelBeforeCreation)
    }
}

interface CleanDatabaseCommandModel: ActiveDatabaseCommandModel

// Domain API

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

sealed interface ActiveDatabaseCommandModel: DatabaseCommandModel, Database {
    override val name: DatabaseName
    override val subscriptionURI: DatabaseSubscriptionURI
    override val created: Instant
    override val revision: DatabaseRevision

    suspend fun eventKeyIsUnique(streamName: StreamName, eventId: EventId): Boolean
    suspend fun satisfiesBatchConstraint(constraint: BatchConstraint): Boolean

    fun acceptBatch(batch: AcceptedBatch): DirtyDatabaseCommandModel =
        DirtyDatabaseCommandModel(this@ActiveDatabaseCommandModel, batch)

    suspend fun delete(): Either<DatabaseDeletionError, DatabaseCommandModelAfterDeletion> =
        DatabaseCommandModelAfterDeletion(this@ActiveDatabaseCommandModel).right()
}

data class NewlyCreatedDatabaseCommandModel internal constructor(
    override val name: DatabaseName,
    override val subscriptionURI: DatabaseSubscriptionURI,
    override val created: Instant,
    private val basis: DatabaseCommandModelBeforeCreation,
): ActiveDatabaseCommandModel {
    override val revision: DatabaseRevision = 0uL

    override suspend fun eventKeyIsUnique(streamName: StreamName, eventId: EventId) = true
    override suspend fun satisfiesBatchConstraint(constraint: BatchConstraint): Boolean =
        when (constraint) {
            is BatchConstraint.StreamExists -> false
            is BatchConstraint.StreamDoesNotExist -> true
            is BatchConstraint.StreamMaxRevision -> constraint.revision == revision
            is BatchConstraint.SubjectExists -> false
            is BatchConstraint.SubjectDoesNotExist -> true
            is BatchConstraint.SubjectMaxRevision -> constraint.revision == revision
            is BatchConstraint.SubjectExistsOnStream -> false
            is BatchConstraint.SubjectDoesNotExistOnStream -> true
            is BatchConstraint.SubjectMaxRevisionOnStream -> constraint.revision == revision
        }
}

data class DirtyDatabaseCommandModel internal constructor(
    private val basis: ActiveDatabaseCommandModel,
    private val batch: AcceptedBatch,
) : ActiveDatabaseCommandModel {
    override val name: DatabaseName
        get() = basis.name
    override val subscriptionURI: DatabaseSubscriptionURI
        get() = basis.subscriptionURI
    override val created: Instant
        get() = basis.created
    override val revision: DatabaseRevision = basis.revision + batch.events.size.toUInt()

    private val eventKeys = batch.events.map { event -> Pair(event.stream, event.id) }.toSet()

    private val streamRevision: Map<StreamName, StreamRevision> =
        batch.events.fold(mutableMapOf<StreamName, StreamRevision>()) { acc, event ->
            val key = event.stream
            acc[key] = event.revision
            acc
        }.toMap()

    private val subjectRevision: Map<EventSubject, StreamRevision> =
        batch.events.fold(mutableMapOf<EventSubject, StreamRevision>()) { acc, event ->
            val subject = event.subject
            if (subject != null) {
                acc[subject] = event.revision
            }
            acc
        }.toMap()

    private val subjectOnStreamRevision: Map<Pair<StreamName, EventSubject>, StreamRevision> =
        batch.events.fold(mutableMapOf<Pair<StreamName, EventSubject>, StreamRevision>()) { acc, event ->
            val subject = event.subject
            if (subject != null) {
                acc[Pair(event.stream, subject)] = event.revision
            }
            acc
        }.toMap()

    val dirtyRelativeToRevision: DatabaseRevision
        get() = if (basis is DirtyDatabaseCommandModel) {
            basis.dirtyRelativeToRevision
        } else {
            basis.revision
        }
    val dirtyBatches: List<AcceptedBatch>
        get() = if (basis is DirtyDatabaseCommandModel) {
            val result = basis.dirtyBatches.toMutableList()
            result.add(batch)
            result.toList()
        } else {
            listOf(batch)
        }

    override suspend fun eventKeyIsUnique(streamName: StreamName, eventId: EventId): Boolean =
        if (eventKeys.contains(Pair(streamName, eventId))) false else basis.eventKeyIsUnique(streamName, eventId)

    override suspend fun satisfiesBatchConstraint(constraint: BatchConstraint): Boolean =
        when (constraint) {
            is BatchConstraint.StreamExists ->
                streamRevision[constraint.stream] != null
                        || basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.StreamDoesNotExist ->
                streamRevision[constraint.stream] == null
                        && basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.StreamMaxRevision ->
                streamRevision[constraint.stream]?.let { it <= constraint.revision }
                    ?: basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.SubjectExists ->
                subjectRevision[constraint.subject] != null
                        || basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.SubjectDoesNotExist ->
                subjectRevision[constraint.subject] == null
                        && basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.SubjectMaxRevision ->
                subjectRevision[constraint.subject]?.let { it <= constraint.revision }
                    ?: basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.SubjectExistsOnStream ->
                subjectOnStreamRevision[Pair(constraint.stream, constraint.subject)] != null
                        || basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.SubjectDoesNotExistOnStream ->
                subjectOnStreamRevision[Pair(constraint.stream, constraint.subject)] == null
                        && basis.satisfiesBatchConstraint(constraint)
            is BatchConstraint.SubjectMaxRevisionOnStream ->
                subjectOnStreamRevision[Pair(constraint.stream, constraint.subject)]?.let { it <= constraint.revision }
                    ?: basis.satisfiesBatchConstraint(constraint)

        }
}

data class DatabaseCommandModelAfterDeletion internal constructor(
    private val basis: ActiveDatabaseCommandModel
): DatabaseCommandModel, Database by basis

// Batch Transaction Lifecycle

//// Event

data class AcceptedEvent private constructor(override val event: CloudEvent): Event {
    companion object {
        operator fun invoke(
            wellFormed: WellFormedProposedEvent,
            database: ActiveDatabaseCommandModel,
            indexInBatch: UInt,
            timestamp: Instant
        ): Either<NonEmptyList<EventInvalidation>, AcceptedEvent> = runBlocking { // TODO: handle better
            either {
                zipOrAccumulate(
                    {
                        ensure(database.eventKeyIsUnique(wellFormed.stream, wellFormed.id)) {
                            DuplicateEventId(wellFormed.stream.value, wellFormed.id.value)
                        }
                    },
                    {

                    }
                ) { _, _ ->
                    val sequence = database.revision + indexInBatch + 1u
                    val newEvent = CloudEventBuilder.from(wellFormed.event)
                        .withExtension(SequenceExtension(sequence))
                        .withTime(timestamp.atOffset(ZoneOffset.UTC))
                        .build()
                    AcceptedEvent(newEvent)
                }
            }
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

data class WellFormedProposedBatch(
    val databaseName: DatabaseName,
    val events: NonEmptyList<WellFormedProposedEvent>,
    val constraints: List<BatchConstraint>
) {
    companion object {
        operator fun invoke(proposedBatch: ProposedBatch): Either<BatchTransactionError, WellFormedProposedBatch> =
            either {
                val nonEmptyEvents = proposedBatch.events.toNonEmptyListOrNull()
                ensureNotNull(nonEmptyEvents) { EmptyBatch }

                val wellFormedEvents = nonEmptyEvents.mapOrAccumulate {
                    it.ensureWellFormed(proposedBatch).bind()
                }.mapLeft { InvalidEventsError(it) }.bind()
                WellFormedProposedBatch(
                    proposedBatch.databasePathEventSourceURI.databaseName,
                    wellFormedEvents,
                    proposedBatch.constraints
                )
            }
    }

    fun accept(
        database: ActiveDatabaseCommandModel,
    ): Either<BatchTransactionError, AcceptedBatch> =
        AcceptedBatch(this, database)
}

data class AcceptedBatch private constructor(
    override val database: DatabaseName,
    val events: NonEmptyList<AcceptedEvent>,
    override val timestamp: Instant,
    override val previousRevision: DatabaseRevision,
): Batch {
    override val eventRevisions
        get() = events.map { it.revision }

    companion object {
        operator fun invoke(
            wellFormed: WellFormedProposedBatch,
            database: ActiveDatabaseCommandModel,
        ): Either<BatchTransactionError, AcceptedBatch> = runBlocking { // TODO: handle better
            either {
                // Validate stream constraints
                wellFormed.constraints.mapOrAccumulate {
                    ensure(database.satisfiesBatchConstraint(it)) { StreamStateConflict(it) }
                }.mapLeft { StreamStateConflictsError(it) }.bind()

                // Validate constituent events
                val timestamp = Instant.now()
                val events = wellFormed.events.withIndex().mapOrAccumulate { eventWithIndex ->
                    eventWithIndex.value
                        .accept(database, eventWithIndex.index.toUInt(), timestamp)
                        .bind()
                }.mapLeft { InvalidEventsError(it) }.bind().toNonEmptyListOrNull()
                AcceptedBatch(wellFormed.databaseName, events!!, timestamp, database.revision)
            }
        }
    }
}
