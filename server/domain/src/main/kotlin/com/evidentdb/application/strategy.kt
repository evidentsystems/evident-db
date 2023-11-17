package com.evidentdb.application

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import com.evidentdb.event_model.*
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow
import org.valiktor.functions.matches
import org.valiktor.validate

// Tenant

//typealias TenantRevision = Long

@JvmInline
value class TenantName private constructor(val value: String) {
    companion object {
        fun build(value: String): TenantName =
            validate(TenantName(value)) {
                validate(TenantName::value).matches(Regex(NAME_PATTERN))
            }
    }
}

fun databaseOutputTopic(
    tenantName: TenantName,
    databaseName: DatabaseName,
) = "evidentdb-${tenantName.value}-${databaseName.value}-event-log"

sealed interface StreamKey {
    val database: DatabaseName
    val streamName: StreamName
    val revision: StreamRevision?

    fun streamPrefix(): String

    fun toBytes() =
        if (revision == null)
            throw IllegalStateException("Cannot serialize to bytes when revision is null")
        else
            "${streamPrefix()}${revision!!.toBase32HexString()}".toByteArray(ENCODING_CHARSET)

    companion object {
        val ENCODING_CHARSET = Charsets.UTF_8

        fun fromBytes(bytes: ByteArray): StreamKey {
            val string = bytes.toString(ENCODING_CHARSET)
            val split = string.split('/', '@')
            return when(split.size) {
                3 -> BaseStreamKey(
                    DatabaseName.build(split[0]),
                    StreamName.build((split[1])),
                    base32HexStringToLong(split[2])
                )
                4 -> SubjectStreamKey(
                    DatabaseName.build(split[0]),
                    StreamName.build((split[1])),
                    EventSubject.build(split[2]),
                    base32HexStringToLong(split[3])
                )
                else -> throw IllegalArgumentException("Invalid StreamKey: $string")
            }
        }
    }
}

data class BaseStreamKey(
    override val database: DatabaseName,
    override val streamName: StreamName,
    override val revision: StreamRevision? = null
): StreamKey {
    override fun streamPrefix(): String =
        "${database.asStreamKeyPrefix()}${streamName.value}@"
}

data class SubjectStreamKey(
    override val database: DatabaseName,
    override val streamName: StreamName,
    val subject: EventSubject,
    override val revision: StreamRevision? = null
): StreamKey {
    override fun streamPrefix(): String =
        "${database.asStreamKeyPrefix()}${streamName.value}/${subject.value}@"
}

interface CommandService {
    val commandManager: CommandManager
    val tenant: TenantName

    suspend fun createDatabase(proposedName: String)
            : Either<DatabaseCreationError, DatabaseCreated> =
        either {
            val name = DatabaseName.of(proposedName).bind()
            val createdTopicName = createDatabaseTopic(
                availableName,
                topic
            ).bind()
            val command = CreateDatabase(
                EnvelopeId.randomUUID(),
                name,
                databaseOutputTopic(tenant, name),
            )
            commandManager.createDatabase(command).bind()
        }

    suspend fun deleteDatabase(nameStr: String)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val name = DatabaseName.of(nameStr).bind()
            val command = DeleteDatabase(
                EnvelopeId.randomUUID(),
                name
            )
            commandManager.deleteDatabase(command).bind()
        }

    suspend fun transactBatch(
        databaseNameStr: String,
        events: Iterable<UnvalidatedProposedEvent>
    ): Either<BatchTransactionError, BatchTransacted> =
        either {
            val databaseName = DatabaseName.of(databaseNameStr).bind()
            val validatedEvents = validateUnvalidatedProposedEvents(
                databaseName,
                events,
            ).bind()
            val command = TransactBatch(
                EnvelopeId.randomUUID(),
                databaseName,
                ProposedBatch(
                    BatchId.randomUUID(),
                    databaseName,
                    validatedEvents
                )
            )
            commandManager.transactBatch(command).bind()
        }
}

interface CommandManager {
    suspend fun createDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated>
    suspend fun deleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted>
    suspend fun transactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted>
}

interface CommandHandler {
    val decider: EvidentDbDecider
    val databaseRepository: DatabaseRepository
    val batchSummaryRepository: BatchSummaryRepository

    suspend fun createDatabaseTopic(database: DatabaseName, topicName: TopicName): Either<DatabaseTopicCreationError, TopicName> =
        topicName.right()

    suspend fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> = either {
       val decision = decider.decide(command).bind()
        createDatabaseTopic(command.database).bind()
        decision
    }

    suspend fun deleteDatabaseTopic(database: DatabaseName, topicName: TopicName): Either<DatabaseTopicDeletionError, Unit> =
        Unit.right()

    suspend fun handleDeleteDatabase(command: DeleteDatabase)
            : Either<DatabaseDeletionError, DatabaseDeleted> =
        either {
            val database = lookupDatabase(
                databaseRepository,
                command.database,
            ).bind()
            deleteDatabaseTopic(
                database.name,
                database.topic,
            ).bind()
            DatabaseDeleted(
                EnvelopeId.randomUUID(),
                command.id,
                database.name,
            )
        }

    suspend fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> =
        either {
            val database = lookupDatabase(
                databaseRepository,
                command.database
            ).bind()
            val validBatch = validateProposedBatch(
                database,
                batchSummaryRepository,
                command.proposedBatch
            ).bind()
            BatchTransacted(
                EnvelopeId.randomUUID(),
                command.id,
                database.name,
                validBatch,
                database,
            )
        }
}

interface QueryService {
    companion object {
        // 0 is the protobuf default for int64 fields if not specified
        const val NO_REVISION_SPECIFIED = 0L
    }

    val databaseRepository: DatabaseRepository
    val batchReadModel: BatchRepository
    val streamRepository: StreamRepository
    val eventRepository: EventRepository

    fun lookupDatabaseMaybeAtRevision(
        name: DatabaseName,
        revision: DatabaseRevision,
    ): ActiveDatabaseCommandModel? =
        if (revision == NO_REVISION_SPECIFIED)
            databaseRepository.database(name)
        else
            databaseRepository.database(name, revision)

    fun getCatalog(): Flow<ActiveDatabaseCommandModel> =
        databaseRepository.catalog()

    suspend fun getDatabase(
        name: String,
        revision: DatabaseRevision = NO_REVISION_SPECIFIED,
    ): Either<DatabaseNotFoundError, ActiveDatabaseCommandModel> = either {
        val validName = DatabaseName.of(name)
            .mapLeft { DatabaseNotFoundError(name) }
            .bind()
        (lookupDatabaseMaybeAtRevision(validName, revision)
            ?.right()
            ?: DatabaseNotFoundError(name).left<DatabaseNotFoundError>())
            .bind()
    }

    suspend fun getDatabaseLog(name: String)
            : Either<DatabaseNotFoundError, Flow<LogBatch>> = either {
        val validName = DatabaseName.of(name)
            .mapLeft { DatabaseNotFoundError(name) }
            .bind()
        (databaseRepository.log(validName)
            ?.right()
            ?: DatabaseNotFoundError(name).left<DatabaseNotFoundError>())
            .bind()
    }

    suspend fun getBatch(database: String, batchId: BatchId)
            : Either<BatchNotFoundError, Batch> = either {
        val notFound = BatchNotFoundError(database, batchId)
        val validName = DatabaseName.of(database)
            .mapLeft { notFound }
            .bind()
        if (databaseRepository.exists(validName)) {
            (batchReadModel.batch(validName, batchId)?.right() ?: notFound.left()).bind()
        } else {
            notFound.left().bind<Batch>()
        }
    }

    suspend fun getEvent(
        database: String,
        eventIndex: EventIndex,
    ) : Either<EventNotFoundError, Pair<EventIndex, CloudEvent>> = either {
        val error = EventNotFoundError(database, eventIndex)
        val validName = DatabaseName.of(database)
            .mapLeft { error }
            .bind()
        if (databaseRepository.exists(validName)) {
            (eventRepository.eventByIndex(validName, eventIndex)
                ?.let { event -> Pair(eventIndex, event) }
                ?.right() ?: error.left())
                .bind()
        } else {
            error.left().bind<Pair<EventIndex, CloudEvent>>()
        }
    }

    suspend fun getStream(
        database: String,
        databaseRevision: DatabaseRevision,
        stream: String,
    ) : Either<StreamNotFoundError, Flow<Pair<StreamRevision, EventIndex>>> = either {
        val databaseName = DatabaseName.of(database)
            .mapLeft { StreamNotFoundError(database, stream) }
            .bind()
        val streamName = StreamName.of(stream)
            .mapLeft { StreamNotFoundError(database, stream) }
            .bind()
        (streamRepository.stream(databaseName, streamName)?.right() ?: StreamNotFoundError(database, stream).left())
            .bind()
    }

    suspend fun getSubjectStream(
        database: String,
        databaseRevision: DatabaseRevision,
        stream: String,
        subject: String,
    ) : Either<StreamNotFoundError, Flow<Pair<StreamRevision, EventIndex>>> = either {
        val databaseName = DatabaseName.of(database)
            .mapLeft { StreamNotFoundError(database, stream) }
            .bind()
        val streamName = StreamName.of(stream)
            .mapLeft { StreamNotFoundError(database, stream) }
            .bind()
        val subjectName = EventSubject.of(subject)
            .mapLeft { StreamNotFoundError(database, stream) }
            .bind()
        (streamRepository.subjectStream(databaseName, streamName, subjectName)
            ?.right()
            ?: StreamNotFoundError(database, stream).left())
            .bind()
    }
}

//object EventHandler {
//    fun databaseUpdate(event: EvidentDbEvent): Pair<DatabaseName, DatabaseOperation> =
//        Pair(event.database, when(event) {
//            is DatabaseCreated -> DatabaseOperation.StoreDatabase(
//                event.data.database.let {
//                    DatabaseSummary(
//                        it.name,
//                        it.topic,
//                        it.created,
//                    )
//                }
//            )
//            is DatabaseDeleted -> DatabaseOperation.DeleteDatabase
//            else -> DatabaseOperation.DoNothing
//        })
//
//    fun batchUpdate(event: EvidentDbEvent): BatchOperation {
//        return when (event) {
//            is BatchTransacted -> BatchOperation.StoreBatch(
//                event.data.batch.let { batch ->
//                    LogBatch(
//                        batch.id,
//                        batch.database,
//                        batch.events.map {
//                            LogBatchEvent(
//                                it.id,
//                                it.stream
//                            )
//                        },
//                        batch.streamRevisions,
//                        batch.timestamp,
//                    )
//                }
//            )
//            is DatabaseDeleted -> BatchOperation.DeleteLog(event.database)
//            else -> BatchOperation.DoNothing
//        }
//    }
//
//    fun streamUpdate(event: EvidentDbEvent): StreamOperation {
//        return when (event) {
//            is BatchTransacted -> {
//                val database = event.data.databaseBefore.name
//                val streamRevisions = event.data.databaseBefore.streamRevisions.toMutableMap()
//                var databaseRevision = event.data.databaseBefore.revision
//                val updates = mutableListOf<Pair<StreamKey, EventIndex>>()
//                for (evt in event.data.batch.events) {
//                    val streamName = evt.stream
//                    databaseRevision += 1
//                    val streamRevision = (streamRevisions[streamName] ?: 0) + 1
//                    streamRevisions[streamName] = streamRevision
//                    updates.add(
//                        Pair(
//                            BaseStreamKey(database, streamName, streamRevision),
//                            databaseRevision
//                        )
//                    )
//                    evt.event.subject?.let {
//                        val subject = EventSubject.build(it)
//                        updates.add(
//                            Pair(
//                                SubjectStreamKey(database, streamName, subject, streamRevision),
//                                databaseRevision
//                            )
//                        )
//                    }
//                }
//                // TODO: remove after verifying
//                require(streamRevisions == event.data.databaseAfter.streamRevisions)
//                StreamOperation.StoreStreams(updates)
//            }
//            is DatabaseDeleted -> StreamOperation.DeleteStreams(event.database)
//            else -> StreamOperation.DoNothing
//        }
//    }
//
//    fun eventsToIndex(event: EvidentDbEvent): List<Pair<EventIndex, Event>>? =
//        when (event) {
//            is BatchTransacted -> event.data.batch.events.map { Pair(it.id, it) }
//            else -> null
//        }
//}
