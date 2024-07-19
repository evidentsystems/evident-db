package com.evidentdb.server.adapter.in_memory

import arrow.core.*
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import com.evidentdb.server.adapter.EvidentDbAdapter
import com.evidentdb.server.application.*
import com.evidentdb.server.domain_model.*
import com.evidentdb.server.event_model.decider
import io.micronaut.context.annotation.Secondary
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

private class InMemoryDatabaseRepository:
    DatabaseCommandModelBeforeCreation,
    WritableDatabaseRepository,
    DatabaseUpdateStream {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(InMemoryDatabaseRepository::class.java)
    }

    private val storage: ConcurrentMap<DatabaseName, InMemoryDatabaseStore> = ConcurrentHashMap()
    // Lifecycle

    override fun setup() = Unit // no-op
    override fun teardown() = Unit // no-op

    override suspend fun databaseNameAvailable(name: DatabaseName): Boolean =
        !storage.containsKey(name)

    override suspend fun databaseCommandModel(name: DatabaseName): DatabaseCommandModel =
        storage[name]?.latestDatabase()?.getOrNull() ?: this

    override suspend fun setupDatabase(database: NewlyCreatedDatabaseCommandModel): Either<DatabaseCreationError, Unit> =
        either {
            LOGGER.info("Setting up database {}", database.name.value)
            val key = database.name
            val value = InMemoryDatabaseStore(database.name)
            // putIfAbsent returns null if no existing key present
            ensure(storage.putIfAbsent(key, value) == null) {
                DatabaseNameAlreadyExists(database.name)
            }
        }

    override suspend fun saveDatabase(
        database: DirtyDatabaseCommandModel
    ): Either<BatchTransactionError, Unit> =
        either {
            LOGGER.info(
                "Saving database {} at revision {}",
                database.name.value, database.revision
            )
            val repository = storage[database.name]
            ensureNotNull(repository) { DatabaseNotFound(database.name.value) }
            repository.saveDatabase(database).bind()
        }

    override suspend fun teardownDatabase(
        database: DatabaseCommandModelAfterDeletion
    ): Either<DatabaseDeletionError, Unit> =
        either {
            LOGGER.info(
                "Tearing down database {} with final revision {}",
                database.name.value, database.revision
            )
            val err = DatabaseNotFound(database.name.value)
            val impl = storage.remove(database.name)
            ensure(impl is InMemoryDatabaseStore) { err }
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
        revision: Revision
    ): Either<DatabaseNotFound, DatabaseReadModel> = either {
        val database = storage[name]
        ensureNotNull(database) { DatabaseNotFound(name.value) }
        database.databaseAtRevision(revision).bind()
    }
}

private class InMemoryDatabaseStore(val name: DatabaseName) {
    private val mutex = Mutex()
    private val storage = TreeMap<InMemoryRepositoryKey, InMemoryRepositoryValue>()
    private val _updates = MutableSharedFlow<Either<DatabaseNotFound, DatabaseRootValue>>(
        extraBufferCapacity = 1000,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )
    val updates: SharedFlow<Either<DatabaseNotFound, Database>> = _updates.asSharedFlow()

    // Initial database state
    init {
        storage[DatabaseRootKey] = DatabaseRootValue(name, 0uL)
    }

    fun latestDatabase(): Either<DatabaseNotFound, InMemoryDatabase> = either {
        val root = storage[DatabaseRootKey]
        ensure(root is DatabaseRootValue) { DatabaseNotFound(name.value) }
        databaseAtRevision(root.revision).bind()
    }

    fun databaseAtRevision(
        revision: Revision
    ): Either<DatabaseNotFound, InMemoryDatabase> =
        InMemoryDatabase(name, revision).right()

    suspend fun saveDatabase(
        newDatabase: DirtyDatabaseCommandModel
    ): Either<BatchTransactionError, Unit> = mutex.withLock("saveDatabase") {
        either {
            // Ensure the current database is in the expected state
            val currentDatabase = latestDatabase().bind()
            ensure(newDatabase.dirtyRelativeToRevision == currentDatabase.revision) {
                ConcurrentWriteCollision(
                    newDatabase.dirtyRelativeToRevision,
                    currentDatabase.revision
                )
            }
            val batch = newDatabase.batch

            // Validate batch constraints against the current database
            batch.constraints.values.mapOrAccumulate { constraint ->
                ensure(currentDatabase.satisfiesBatchConstraint(constraint)) { constraint }
            }.mapLeft {
                BatchConstraintViolations(batch, it)
            }.bind()

            // Validate event ID + stream uniqueness against the current database
            batch.events.mapOrAccumulate { event ->
                ensure(currentDatabase.eventKeyIsUnique(event.stream, event.id)) {
                    InvalidEvent(
                        event.event,
                        nonEmptyListOf(DuplicateEventId(event.stream.value, event.id.value))
                    )
                }
            }.mapLeft { InvalidEvents(it) }.bind()

            // Everything looks good, create DatabaseRootValue and index entries
            val nextDatabaseRoot = DatabaseRootValue(
                newDatabase.name,
                newDatabase.revision
            )
            val transaction = batch.events.foldLeft(
                mutableMapOf<InMemoryRepositoryKey, InMemoryRepositoryValue>(
                    BatchKey(batch.revision) to BatchValue(
                        batch.database,
                        batch.events,
                        batch.timestamp,
                        batch.basis,
                    )
                )
            ) { txn, event ->
                val revision = event.revision
                val stream = event.stream
                txn[EventIdIndexKey(stream, event.id)] = EventIdIndexValue(revision)
                txn[EventStreamIndexKey(stream, revision)] = NullValue
                txn[EventTypeIndexKey(event.type, revision)] = NullValue
                event.subject?.let {
                    txn[EventSubjectStreamIndexKey(stream, it, revision)] = NullValue
                    txn[EventSubjectIndexKey(it, revision)] = NullValue
                }
                txn
            }

            try {
                // Apply the index entries
                storage += transaction
                // And set the database root value to the successor
                storage[DatabaseRootKey] = nextDatabaseRoot
                // Emit the database root to subscribers
                _updates.tryEmit(nextDatabaseRoot.right())
            } catch (e: Exception) {
                // If tryEmit above throws error, remove index entries...
                transaction.forEach {
                    storage.remove(it.key)
                }
                // ...and reset the root value to the current database
                storage[DatabaseRootKey] = DatabaseRootValue(
                    currentDatabase.name,
                    currentDatabase.revision
                )
            }
        }
    }

    fun tearDown() {
        runBlocking {
            _updates.emit(DatabaseNotFound(name.value).left())
        }
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

    private data class BatchKey(val revision: Revision): InMemoryRepositoryKey {
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
            val MIN_VALUE = BatchKey(Revision.MIN_VALUE)
            val MAX_VALUE = BatchKey(Revision.MAX_VALUE)
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
        val revision: Revision,
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
            fun minStreamKey(stream: StreamName) = EventStreamIndexKey(stream, Revision.MIN_VALUE)
        }
    }

    private data class EventSubjectIndexKey(
        val subject: EventSubject,
        val revision: Revision,
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
            fun minSubjectKey(subject: EventSubject) = EventSubjectIndexKey(subject, Revision.MIN_VALUE)
        }
    }

    private data class EventSubjectStreamIndexKey(
        val stream: StreamName,
        val subject: EventSubject,
        val revision: Revision,
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
                EventSubjectStreamIndexKey(stream, subject, Revision.MIN_VALUE)
        }
    }

    private data class EventTypeIndexKey(
        val type: EventType,
        val revision: Revision,
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
            fun minEventTypeKey(type: EventType) = EventTypeIndexKey(type, Revision.MIN_VALUE)
        }
    }

    private sealed interface InMemoryRepositoryValue

    private data object NullValue: InMemoryRepositoryValue

    private data class EventIdIndexValue(val revision: Revision): InMemoryRepositoryValue

    private data class DatabaseRootValue(
        override val name: DatabaseName,
        override val revision: Revision,
    ): Database, InMemoryRepositoryValue

    private data class BatchValue(
        override val database: DatabaseName,
        override val events: NonEmptyList<Event>,
        override val timestamp: Instant,
        override val basis: Revision,
    ): BatchDetail, InMemoryRepositoryValue {
        override val revision: Revision
            get() = basis + events.size.toUInt()

        fun getEventByAbsoluteRevision(rev: Revision): Event? =
            try {
                events[(rev - basis).toInt() - 1]
            } catch (e: IndexOutOfBoundsException) {
                null
            }
    }

    inner class InMemoryDatabase(
        override val name: DatabaseName,
        override val revision: Revision
    ): CleanDatabaseCommandModel, DatabaseReadModel {
        fun eventKeyIsUnique(streamName: StreamName, eventId: EventId): Boolean =
            storage[EventIdIndexKey(streamName, eventId)] == null

        fun satisfiesBatchConstraint(constraint: BatchConstraint): Boolean =
            when (constraint) {
                is BatchConstraint.DatabaseMinRevision -> constraint.revision <= revision
                is BatchConstraint.DatabaseMaxRevision -> revision <= constraint.revision
                is BatchConstraint.DatabaseRevisionRange ->
                    constraint.min <= revision && revision <= constraint.max

                is BatchConstraint.StreamMinRevision -> {
                    val match: InMemoryRepositoryKey? = storage.ceilingKey(
                        EventStreamIndexKey(constraint.stream, constraint.revision)
                    )
                    match is EventStreamIndexKey && match.stream == constraint.stream
                }
                is BatchConstraint.StreamMaxRevision -> {
                    val match = storage.ceilingKey(
                        EventStreamIndexKey(constraint.stream, constraint.revision + 1u)
                    )
                    match !is EventStreamIndexKey || match.stream != constraint.stream
                }
                is BatchConstraint.StreamRevisionRange -> {
                    val minKey = storage.ceilingKey(
                        EventStreamIndexKey(constraint.stream, constraint.min)
                    )
                    val maxKey = storage.ceilingKey(
                        EventStreamIndexKey(constraint.stream, constraint.max + 1u)
                    )
                    (minKey is EventStreamIndexKey && minKey.stream == constraint.stream)
                            &&
                            (maxKey !is EventStreamIndexKey || maxKey.stream != constraint.stream)
                }

                is BatchConstraint.SubjectMinRevision -> {
                    val match = storage.ceilingKey(
                        EventSubjectIndexKey(constraint.subject, constraint.revision)
                    )
                    match is EventSubjectIndexKey && match.subject == constraint.subject
                }
                is BatchConstraint.SubjectMaxRevision -> {
                    val match = storage.ceilingKey(
                        EventSubjectIndexKey(constraint.subject, constraint.revision + 1u)
                    )
                    match !is EventSubjectIndexKey || match.subject != constraint.subject
                }
                is BatchConstraint.SubjectRevisionRange -> {
                    val minKey = storage.ceilingKey(
                        EventSubjectIndexKey(constraint.subject, constraint.min)
                    )
                    val maxKey = storage.ceilingKey(
                        EventSubjectIndexKey(constraint.subject, constraint.max + 1u)
                    )
                    (minKey is EventSubjectIndexKey && minKey.subject == constraint.subject)
                            &&
                            (maxKey !is EventSubjectIndexKey || maxKey.subject != constraint.subject)
                }

                is BatchConstraint.SubjectMinRevisionOnStream -> {
                    val match = storage.ceilingKey(
                        EventSubjectStreamIndexKey(
                            constraint.stream,
                            constraint.subject,
                            constraint.revision
                        )
                    )
                    match is EventSubjectStreamIndexKey
                            && match.stream == constraint.stream
                            && match.subject == constraint.subject
                }
                is BatchConstraint.SubjectMaxRevisionOnStream -> {
                    val match = storage.ceilingKey(
                        EventSubjectStreamIndexKey(
                            constraint.stream,
                            constraint.subject,
                            constraint.revision + 1u
                        )
                    )
                    match !is EventSubjectStreamIndexKey ||
                            match.stream != constraint.stream ||
                            match.subject != constraint.subject
                }
                is BatchConstraint.SubjectStreamRevisionRange -> {
                    val minKey = storage.ceilingKey(
                        EventSubjectStreamIndexKey(
                            constraint.stream,
                            constraint.subject,
                            constraint.min
                        )
                    )
                    val maxKey = storage.ceilingKey(
                        EventSubjectStreamIndexKey(
                            constraint.stream,
                            constraint.subject,
                            constraint.max + 1u
                        )
                    )
                    (minKey is EventSubjectStreamIndexKey
                            && minKey.stream == constraint.stream
                            && minKey.subject == constraint.subject) &&
                            (maxKey !is EventSubjectStreamIndexKey ||
                                    maxKey.stream != constraint.stream ||
                                    maxKey.subject != constraint.subject)
                }
            }

        override fun log(startAtRevision: Revision): Flow<Batch> = logDetail(startAtRevision)

        override fun logDetail(startAtRevision: Revision): Flow<BatchDetail> =
            // From minimum batch key (exclusive) to batch key as of this DB's revision
            storage.subMap(BatchKey(startAtRevision), true, BatchKey(revision), true)
                .values
                .filterIsInstance<BatchValue>()
                .asFlow()

        override suspend fun eventById(stream: StreamName, id: EventId): Either<EventNotFound, Event> = either {
            val indexValue = storage[EventIdIndexKey(stream, id)]
            val notFound = EventNotFound("No event with id=$id and stream=$stream found in database: ${name.value}")
            ensureNotNull(indexValue) { notFound }
            ensure(indexValue is EventIdIndexValue) { notFound }
            // Ensure event revision is in scope for this DB's revision
            ensure(indexValue.revision <= revision) { notFound }
            val result = eventByRevision(indexValue.revision)
            ensureNotNull(result) { notFound }
            result
        }

        override fun stream(stream: StreamName): Flow<Revision> =
            storage.subMap(
                EventStreamIndexKey.minStreamKey(stream), false,
                EventStreamIndexKey(stream, revision), true
            )
                .keys
                .filterIsInstance<EventStreamIndexKey>()
                .filter { it.revision <= revision }
                .map { it.revision }
                .asFlow()

        override fun streamDetail(stream: StreamName): Flow<Event> =
            stream(stream).map { eventByRevision(it)!! }

        override fun subjectStream(stream: StreamName, subject: EventSubject): Flow<Revision> =
            storage.subMap(
                EventSubjectStreamIndexKey.minSubjectStreamKey(stream, subject), false,
                EventSubjectStreamIndexKey(stream, subject, revision), true
            )
                .keys
                .filterIsInstance<EventSubjectStreamIndexKey>()
                .filter { it.revision <= revision }
                .map { it.revision }
                .asFlow()

        override fun subjectStreamDetail(
            stream: StreamName,
            subject: EventSubject
        ): Flow<Event> =
            subjectStream(stream, subject).map { eventByRevision(it)!! }

        override fun subject(subject: EventSubject): Flow<Revision> =
            storage.subMap(
                EventSubjectIndexKey.minSubjectKey(subject), false,
                EventSubjectIndexKey(subject, revision), true
            )
                .keys
                .filterIsInstance<EventSubjectIndexKey>()
                .filter { it.revision <= revision }
                .map { it.revision }
                .asFlow()

        override fun subjectDetail(subject: EventSubject): Flow<Event> =
            subject(subject).map { eventByRevision(it)!! }

        override fun eventType(type: EventType): Flow<Revision> =
            storage.subMap(
                EventTypeIndexKey.minEventTypeKey(type), false,
                EventTypeIndexKey(type, revision), true
            )
                .keys
                .filterIsInstance<EventTypeIndexKey>()
                .filter { it.revision <= revision }
                .map { it.revision }
                .asFlow()

        override fun eventTypeDetail(type: EventType): Flow<Event> =
            eventType(type).map { eventByRevision(it)!! }

        private fun eventByRevision(
            revision: Revision
        ): Event? = storage.ceilingEntry(BatchKey(revision)).let {
            val batch = it.value
            if (batch is BatchValue) {
                batch.getEventByAbsoluteRevision(revision)
            } else {
                throw IllegalStateException("Value stored under BatchKey isn't a BatchValue")
            }
        }

        override fun eventsByRevision(
            revisions: List<Revision>
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

@Secondary
@Singleton
class InMemoryAdapter: EvidentDbAdapter {
    override val commandService: CommandService
    override val databaseUpdateStream: DatabaseUpdateStream
    override val repository: DatabaseRepository

    companion object {
        private val LOGGER = LoggerFactory.getLogger(InMemoryAdapter::class.java)
    }

    init {
        val repo = InMemoryDatabaseRepository()
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
        LOGGER.info("Setting up InMemoryAdapter...")
        super.setup()
        LOGGER.info("...Finished setting up InMemoryAdapter.")
    }

    @PreDestroy
    fun preDestroy() {
        LOGGER.info("Tearing down InMemoryAdapter...")
        super.teardown()
        LOGGER.info("...Finished tearing down InMemoryAdapter.")
    }
}
