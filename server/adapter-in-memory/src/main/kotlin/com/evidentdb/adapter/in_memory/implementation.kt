package com.evidentdb.adapter.in_memory

import arrow.core.*
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import com.evidentdb.adapter.EvidentDbAdapter
import com.evidentdb.application.*
import com.evidentdb.domain_model.*
import com.evidentdb.event_model.decider
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import kotlinx.coroutines.flow.*
import java.lang.IndexOutOfBoundsException
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private class InMemoryCatalogRepository:
    DatabaseCommandModelBeforeCreation,
    WritableDatabaseRepository,
    DatabaseUpdateStream {
    private val storage: ConcurrentMap<DatabaseName, InMemoryDatabaseRepository> = ConcurrentHashMap()
    // Lifecycle
    override fun setup() = Unit // no-op
    override fun teardown() = Unit // no-op

    override suspend fun databaseNameAvailable(name: DatabaseName): Boolean =
        !storage.containsKey(name)

    override suspend fun databaseCommandModel(name: DatabaseName): DatabaseCommandModel =
        storage[name]?.latestDatabase()?.getOrNull() ?: this

    override suspend fun setupDatabase(database: NewlyCreatedDatabaseCommandModel): Either<DatabaseCreationError, Unit> =
        either {
            val key = database.name
            val value = InMemoryDatabaseRepository(database.name)
            // putIfAbsent returns null if no existing key present
            ensure(storage.putIfAbsent(key, value) == null) {
                DatabaseNameAlreadyExists(database.name)
            }
        }

    override suspend fun saveDatabase(database: DirtyDatabaseCommandModel): Either<BatchTransactionError, Unit> =
        either {
            val repository = storage[database.name]
            ensureNotNull(repository) { DatabaseNotFound(database.name.value) }
            repository.saveDatabase(database).bind()
        }

    override suspend fun teardownDatabase(
        database: DatabaseCommandModelAfterDeletion
    ): Either<DatabaseDeletionError, Unit> =
        either {
            val err = DatabaseNotFound(database.name.value)
            val impl = storage.remove(database.name)
            ensure(impl is InMemoryDatabaseRepository) { err }
            impl.tearDown()
        }

    override fun databaseCatalog(): Flow<DatabaseName> =
        storage
            .values
            .mapNotNull { it.name }
            .asFlow()

    override fun subscribe(name: DatabaseName): Flow<Either<DatabaseNotFound, Database>> = flow {
        when (val database = storage[name]) {
            null -> emit(DatabaseNotFound(name.value).left())
            else -> emitAll(database.updates)
        }
    }

    override suspend fun latestDatabase(name: DatabaseName): Either<DatabaseNotFound, DatabaseReadModel> = either {
        val database = storage[name]
        ensureNotNull(database) { DatabaseNotFound(name.value) }
        database.latestDatabase().bind()
    }

    override suspend fun databaseAtRevision(
        name: DatabaseName,
        revision: DatabaseRevision
    ): Either<DatabaseNotFound, DatabaseReadModel> = either {
        val database = storage[name]
        ensureNotNull(database) { DatabaseNotFound(name.value) }
        database.databaseAtRevision(revision).bind()
    }
}

private class InMemoryDatabaseRepository(
    val name: DatabaseName,
) {
    private val lock = ReentrantLock()
    private val storage: NavigableMap<InMemoryRepositoryKey, InMemoryRepositoryValue>
    private val _updates: MutableStateFlow<Either<DatabaseNotFound, Database>>
    val updates: StateFlow<Either<DatabaseNotFound, Database>>

    // Initial database state
    init {
        val initialDatabase = DatabaseRootValue(name, 0uL)
        storage = TreeMap()
        storage[DatabaseRootKey] = initialDatabase
        _updates = MutableStateFlow(initialDatabase.right())
        updates = _updates.asStateFlow()
    }

    fun latestDatabase(): Either<DatabaseNotFound, InMemoryDatabase> = either {
        val root = storage[DatabaseRootKey]
        ensure(root is DatabaseRootValue) { DatabaseNotFound(name.value) }
        databaseAtRevision(root.revision).bind()
    }

    fun databaseAtRevision(
        revision: DatabaseRevision
    ): Either<DatabaseNotFound, InMemoryDatabase> =
        InMemoryDatabase(name, revision).right()

    fun saveDatabase(database: DirtyDatabaseCommandModel): Either<BatchTransactionError, Unit> = lock.withLock {
        either {
            val currentDatabaseRoot = storage[DatabaseRootKey]
            ensure(currentDatabaseRoot is DatabaseRootValue) { DatabaseNotFound(database.name.value) }
            ensure(database.dirtyRelativeToRevision == currentDatabaseRoot.revision) {
                ConcurrentWriteCollision(database.dirtyRelativeToRevision, currentDatabaseRoot.revision)
            }
            val nextDatabaseRoot = DatabaseRootValue(
                database.name,
                database.revision
            )
            val batchAndIndexes = mutableMapOf<InMemoryRepositoryKey, InMemoryRepositoryValue>()
            database.dirtyBatches.forEach {
                addBatchToTransaction(batchAndIndexes, it)
            }
            try {
                storage.plusAssign(batchAndIndexes)
                storage[DatabaseRootKey] = nextDatabaseRoot
                val updateSent = _updates.compareAndSet(currentDatabaseRoot.right(), nextDatabaseRoot.right())
                if (!updateSent) {
                    throw IllegalStateException("Compare and set to updates state flow failed")
                }
            } catch (t: Throwable) {
                batchAndIndexes.forEach {
                    storage.remove(it.key)
                }
                storage[DatabaseRootKey] = currentDatabaseRoot
                _updates.value = currentDatabaseRoot.right() // TODO: is this the right semantic here?
            }
        }
    }

    private fun addBatchToTransaction(
        transaction: MutableMap<InMemoryRepositoryKey, InMemoryRepositoryValue>,
        batch: AcceptedBatch
    ) {
        transaction[BatchKey(batch.revision)] = BatchValue(
            batch.database,
            batch.events,
            batch.timestamp,
            batch.basisRevision,
        )
        batch.events.forEach { event ->
            val revision = event.revision
            val stream = event.stream
            transaction[EventIdIndexKey(stream, event.id)] = EventIdIndexValue(revision)
            transaction[EventStreamIndexKey(stream, revision)] = NullValue
            transaction[EventTypeIndexKey(event.type, revision)] = NullValue
            event.subject?.let {
                transaction[EventSubjectStreamIndexKey(stream, it, revision)] = NullValue
                transaction[EventSubjectIndexKey(it, revision)] = NullValue
            }
        }
    }

    fun tearDown() {
        _updates.value = DatabaseNotFound(name.value).left()
    }

    // Let's compare these keys thus: DatabaseRootKey object first, then the others sorted
    // alphabetically by class name, and per attributes between instances of same class
    private sealed interface InMemoryRepositoryKey: Comparable<InMemoryRepositoryKey>

    private data object DatabaseRootKey: InMemoryRepositoryKey {
        override fun compareTo(other: InMemoryRepositoryKey): Int =
            when (other) {
                DatabaseRootKey -> 0
                else -> Int.MIN_VALUE
            }
    }

    private data class BatchKey(val revision: DatabaseRevision): InMemoryRepositoryKey {
        override fun compareTo(other: InMemoryRepositoryKey): Int =
            when (other) {
                DatabaseRootKey -> Int.MAX_VALUE
                is BatchKey -> revision.compareTo(other.revision)
                is EventIdIndexKey -> Int.MIN_VALUE
                is EventStreamIndexKey -> Int.MIN_VALUE
                is EventSubjectIndexKey -> Int.MIN_VALUE
                is EventSubjectStreamIndexKey -> Int.MIN_VALUE
                is EventTypeIndexKey -> Int.MIN_VALUE
            }

        companion object {
            val MIN_VALUE = BatchKey(DatabaseRevision.MIN_VALUE)
            val MAX_VALUE = BatchKey(DatabaseRevision.MAX_VALUE)
        }
    }

    private data class EventIdIndexKey(
        val stream: StreamName,
        val id: EventId,
    ): InMemoryRepositoryKey {
        override fun compareTo(other: InMemoryRepositoryKey): Int =
            when (other) {
                DatabaseRootKey -> Int.MAX_VALUE
                is BatchKey -> Int.MAX_VALUE
                is EventIdIndexKey ->
                    Pair(id.value, stream.value).compareTo(Pair(other.id.value, other.stream.value))
                is EventStreamIndexKey -> Int.MIN_VALUE
                is EventSubjectStreamIndexKey -> Int.MIN_VALUE
                is EventSubjectIndexKey -> Int.MIN_VALUE
                is EventTypeIndexKey -> Int.MIN_VALUE
            }
    }

    private data class EventStreamIndexKey(
        val stream: StreamName,
        val revision: DatabaseRevision,
    ): InMemoryRepositoryKey {
        override fun compareTo(other: InMemoryRepositoryKey): Int =
            when (other) {
                DatabaseRootKey -> Int.MAX_VALUE
                is BatchKey -> Int.MAX_VALUE
                is EventIdIndexKey -> Int.MAX_VALUE
                is EventStreamIndexKey ->
                    Pair(stream.value, revision).compareTo(Pair(other.stream.value, other.revision))
                is EventSubjectIndexKey -> Int.MIN_VALUE
                is EventSubjectStreamIndexKey -> Int.MIN_VALUE
                is EventTypeIndexKey -> Int.MIN_VALUE
            }

        companion object {
            fun minStreamKey(stream: StreamName) = EventStreamIndexKey(stream, DatabaseRevision.MIN_VALUE)
            fun maxStreamKey(stream: StreamName) = EventStreamIndexKey(stream, DatabaseRevision.MAX_VALUE)
        }
    }

    private data class EventSubjectIndexKey(
        val subject: EventSubject,
        val revision: DatabaseRevision,
    ): InMemoryRepositoryKey {
        override fun compareTo(other: InMemoryRepositoryKey): Int =
            when (other) {
                DatabaseRootKey -> Int.MAX_VALUE
                is BatchKey -> Int.MAX_VALUE
                is EventIdIndexKey -> Int.MAX_VALUE
                is EventStreamIndexKey -> Int.MAX_VALUE
                is EventSubjectIndexKey ->
                    Pair(subject.value, revision).compareTo(Pair(other.subject.value, other.revision))
                is EventSubjectStreamIndexKey -> Int.MIN_VALUE
                is EventTypeIndexKey -> Int.MIN_VALUE
            }

        companion object {
            fun minSubjectKey(subject: EventSubject) = EventSubjectIndexKey(subject, DatabaseRevision.MIN_VALUE)
            fun maxSubjectKey(subject: EventSubject) = EventSubjectIndexKey(subject, DatabaseRevision.MAX_VALUE)
        }
    }

    private data class EventSubjectStreamIndexKey(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: DatabaseRevision,
    ): InMemoryRepositoryKey {
        override fun compareTo(other: InMemoryRepositoryKey): Int =
            when (other) {
                DatabaseRootKey -> Int.MAX_VALUE
                is BatchKey -> Int.MAX_VALUE
                is EventIdIndexKey -> Int.MAX_VALUE
                is EventStreamIndexKey -> Int.MAX_VALUE
                is EventSubjectIndexKey -> Int.MAX_VALUE
                is EventSubjectStreamIndexKey ->
                    Triple(stream.value, subject.value, revision).compareTo(
                        Triple(other.stream.value, other.subject.value, other.revision)
                    )
                is EventTypeIndexKey -> Int.MIN_VALUE
            }

        companion object {
            fun minSubjectStreamKey(stream: StreamName, subject: EventSubject) =
                EventSubjectStreamIndexKey(stream, subject, DatabaseRevision.MIN_VALUE)
            fun maxSubjectStreamKey(stream: StreamName, subject: EventSubject) =
                EventSubjectStreamIndexKey(stream, subject, DatabaseRevision.MAX_VALUE)
        }
    }

    private data class EventTypeIndexKey(
        val type: EventType,
        val revision: DatabaseRevision,
    ): InMemoryRepositoryKey {
        override fun compareTo(other: InMemoryRepositoryKey): Int =
            when (other) {
                DatabaseRootKey -> Int.MAX_VALUE
                is BatchKey -> Int.MAX_VALUE
                is EventIdIndexKey -> Int.MAX_VALUE
                is EventStreamIndexKey -> Int.MAX_VALUE
                is EventSubjectIndexKey -> Int.MAX_VALUE
                is EventSubjectStreamIndexKey -> Int.MAX_VALUE
                is EventTypeIndexKey ->
                    Pair(type.value, revision).compareTo(Pair(other.type.value, other.revision))
            }

        companion object {
            fun minEventTypeKey(type: EventType) = EventTypeIndexKey(type, DatabaseRevision.MIN_VALUE)
            fun maxEventTypeKey(type: EventType) = EventTypeIndexKey(type, DatabaseRevision.MAX_VALUE)
        }
    }

    private sealed interface InMemoryRepositoryValue

    private data object NullValue: InMemoryRepositoryValue

    private data class EventIdIndexValue(val revision: DatabaseRevision): InMemoryRepositoryValue

    private data class DatabaseRootValue(
        override val name: DatabaseName,
        override val revision: DatabaseRevision,
    ): Database, InMemoryRepositoryValue

    private data class BatchValue(
        override val database: DatabaseName,
        val events: NonEmptyList<Event>,
        override val timestamp: Instant,
        override val basisRevision: DatabaseRevision,
    ): Batch, InMemoryRepositoryValue {
        override val eventRevisions: NonEmptyList<EventRevision>
            get() = events.map { it.revision }

        fun getEventByAbsoluteRevision(revision: Revision): Event? =
            try {
                events[(revision - basisRevision).toInt() - 1]
            } catch (e: IndexOutOfBoundsException) {
                null
            }
    }

    inner class InMemoryDatabase(
        override val name: DatabaseName,
        override val revision: DatabaseRevision
    ): CleanDatabaseCommandModel, DatabaseReadModel {
        override suspend fun eventKeyIsUnique(streamName: StreamName, eventId: EventId): Boolean =
            eventById(streamName, eventId).isLeft()

        override suspend fun satisfiesBatchConstraint(constraint: BatchConstraint): Boolean =
            when (constraint) {
                is BatchConstraint.StreamExists ->
                    storage.subMap(
                        EventStreamIndexKey.minStreamKey(constraint.stream), false,
                        EventStreamIndexKey.maxStreamKey(constraint.stream), true
                    ).isNotEmpty()
                is BatchConstraint.StreamDoesNotExist ->
                    storage.subMap(
                        EventStreamIndexKey.minStreamKey(constraint.stream), false,
                        EventStreamIndexKey.maxStreamKey(constraint.stream), true
                    ).isEmpty()
                is BatchConstraint.StreamMaxRevision -> {
                    val key = EventStreamIndexKey(constraint.stream, constraint.revision)
                    val result = storage.ceilingKey(key)
                    result == null || result == key
                }
                is BatchConstraint.SubjectExists ->
                    storage.subMap(
                        EventSubjectIndexKey.minSubjectKey(constraint.subject), false,
                        EventSubjectIndexKey.maxSubjectKey(constraint.subject), true
                    ).isNotEmpty()
                is BatchConstraint.SubjectDoesNotExist ->
                    storage.subMap(
                        EventSubjectIndexKey.minSubjectKey(constraint.subject), false,
                        EventSubjectIndexKey.maxSubjectKey(constraint.subject), true
                    ).isEmpty()
                is BatchConstraint.SubjectMaxRevision -> {
                    val key = EventSubjectIndexKey(constraint.subject, constraint.revision)
                    val result = storage.ceilingKey(key)
                    result == null || result == key
                }
                is BatchConstraint.SubjectExistsOnStream ->
                    storage.subMap(
                        EventSubjectStreamIndexKey.minSubjectStreamKey(constraint.stream, constraint.subject),
                        false,
                        EventSubjectStreamIndexKey.maxSubjectStreamKey(constraint.stream, constraint.subject),
                        true
                    ).isNotEmpty()
                is BatchConstraint.SubjectDoesNotExistOnStream ->
                    storage.subMap(
                        EventSubjectStreamIndexKey.minSubjectStreamKey(constraint.stream, constraint.subject),
                        false,
                        EventSubjectStreamIndexKey.maxSubjectStreamKey(constraint.stream, constraint.subject),
                        true
                    ).isEmpty()
                is BatchConstraint.SubjectMaxRevisionOnStream -> {
                    val key = EventSubjectStreamIndexKey(constraint.stream, constraint.subject, constraint.revision)
                    val result = storage.ceilingKey(key)
                    result == null || result == key
                }
            }

        override fun log(): Flow<Batch> =
            storage.subMap(BatchKey.MIN_VALUE, false, BatchKey.MAX_VALUE, true)
                .values
                .filterIsInstance<BatchValue>()
                .asFlow()

        override suspend fun eventById(stream: StreamName, id: EventId): Either<EventNotFound, Event> = either {
            val indexValue = storage[EventIdIndexKey(stream, id)]
            val notFound = EventNotFound("No event with id=$id and stream=$stream found in database: ${name.value}")
            ensureNotNull(indexValue) { notFound }
            ensure(indexValue is EventIdIndexValue) { notFound }
            val result = eventByRevision(indexValue.revision)
            ensureNotNull(result) { notFound }
            result
        }

        override fun stream(stream: StreamName): Flow<EventRevision> =
            storage.subMap(
                EventStreamIndexKey.minStreamKey(stream), false,
                EventStreamIndexKey.maxStreamKey(stream), true
            )
                .keys
                .filterIsInstance<EventStreamIndexKey>()
                .map { it.revision }
                .asFlow()

        override fun subjectStream(stream: StreamName, subject: EventSubject): Flow<EventRevision> =
            storage.subMap(
                EventSubjectStreamIndexKey.minSubjectStreamKey(stream, subject), false,
                EventSubjectStreamIndexKey.maxSubjectStreamKey(stream, subject), true
            )
                .keys
                .filterIsInstance<EventSubjectStreamIndexKey>()
                .map { it.revision }
                .asFlow()

        override fun subject(subject: EventSubject): Flow<EventRevision> =
            storage.subMap(
                EventSubjectIndexKey.minSubjectKey(subject), false,
                EventSubjectIndexKey.maxSubjectKey(subject), true
            )
                .keys
                .filterIsInstance<EventSubjectIndexKey>()
                .map { it.revision }
                .asFlow()

        override fun eventType(type: EventType): Flow<EventRevision> =
            storage.subMap(
                EventTypeIndexKey.minEventTypeKey(type), false,
                EventTypeIndexKey.maxEventTypeKey(type), true
            )
                .keys
                .filterIsInstance<EventTypeIndexKey>()
                .map { it.revision }
                .asFlow()

        private fun eventByRevision(
            revision: DatabaseRevision
        ): Event? = storage.ceilingEntry(BatchKey(revision)).let {
            val batch = it.value
            if (batch is BatchValue) {
                batch.getEventByAbsoluteRevision(revision)
            } else {
                throw IllegalStateException("Value stored under BatchKey isn't a BatchValue")
            }
        }

        override fun eventsByRevision(
            revisions: List<EventRevision>
        ): Flow<Either<QueryError, Event>> = flow {
            revisions.forEach {
                val event = eventByRevision(it)
                if (event == null) {
                    emit(EventNotFound("Event with revision $it not found in database $name").left())
                } else {
                    emit(event.right())
                }
            }
        }
    }
}

@Singleton
class InMemoryAdapter: EvidentDbAdapter {
    override val commandService: CommandService
    override val databaseUpdateStream: DatabaseUpdateStream
    override val repository: DatabaseRepository

    init {
        val repo = InMemoryCatalogRepository()
        databaseUpdateStream = repo
        commandService = CommandService(
            decider(repo),
            repo,
            EmptyPathEventSourceURI("evidentdb://mem")
                .getOrElse { throw IllegalArgumentException("Invalid EmptyPathEventSourceURI") },
        )
        repository = repo
    }

    @PostConstruct
    fun postConstruct() {
        super.setup()
    }

    @PreDestroy
    fun preDestroy() {
        super.teardown()
    }
}
