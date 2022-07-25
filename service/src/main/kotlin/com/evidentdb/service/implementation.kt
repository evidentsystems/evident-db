package com.evidentdb.service

import kotlin.concurrent.thread
import kotlin.coroutines.*
import java.util.concurrent.ConcurrentHashMap
import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain.*
import com.evidentdb.domain.CommandBroker
import com.evidentdb.kafka.DatabaseReadModelStore
import kotlinx.coroutines.CompletableDeferred
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import java.lang.RuntimeException
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

private suspend inline fun <reified K: Any, reified V : Any> KafkaProducer<K, V>.publish(record: ProducerRecord<K, V>) =
    suspendCoroutine<RecordMetadata> { continuation ->
        val callback = Callback { metadata, exception ->
            if (metadata == null) {
                continuation.resumeWithException(exception!!)
            } else {
                continuation.resume(metadata)
            }
        }
        this.send(record, callback)
    }

class KafkaCommandBroker(
    producerConfig: ProducerConfig,
    consumerConfig: ConsumerConfig,
    timeout: Duration,
    private val commandTopic: String,
    private val eventTopic: String,
): CommandBroker {
    private val running = AtomicBoolean(true)
    private val inFlight = ConcurrentHashMap<CommandId, CompletableDeferred<EventEnvelope>>()
    private val producer = KafkaProducer<CommandId, CommandEnvelope>(producerConfig.values())
    private val consumer = KafkaConsumer<EventId,   EventEnvelope>  (consumerConfig.values())

    // TODO: close/cleanup when done
    init {
        thread {
            consumer.subscribe(listOf(eventTopic))
            while (running.get()) {
                consumer.poll(timeout).forEach { record ->
                    inFlight.remove(record.value().commandId)?.complete(record.value())
                }
            }
        }
    }

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> = either {
        val deferred = publishCommand(command).bind()

        // TODO: flatten this such that DatabaseCreationError is sibling of DatabaseCreated event, and only one `else` branch is need
        when(val result = deferred.await()) {
            is DatabaseCreated -> result.right()
            is ErrorEnvelope -> when(val body = result.data){
                is DatabaseCreationError -> body.left()
                else -> InternalServerError("Invalid result of createDatabase: $result").left()
            }
            else -> InternalServerError("Invalid result of createDatabase: $result").left()
        }.bind()
    }

    override suspend fun renameDatabase(command: RenameDatabase): Either<DatabaseRenameError, DatabaseRenamed> = either {
        val deferred = publishCommand(command).bind()

        // TODO: flatten this
        when(val result = deferred.await()) {
            is DatabaseRenamed -> result.right()
            is ErrorEnvelope -> when(val body = result.data){
                is DatabaseRenameError -> body.left()
                else -> InternalServerError("Invalid result of createDatabase: $result").left()
            }
            else -> InternalServerError("Invalid result of createDatabase: $result").left()
        }.bind()
    }

    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> = either {
        val deferred = publishCommand(command).bind()

        // TODO: flatten this
        when(val result = deferred.await()) {
            is DatabaseDeleted -> result.right()
            is ErrorEnvelope -> when(val body = result.data){
                is DatabaseDeletionError -> body.left()
                else -> InternalServerError("Invalid result of createDatabase: $result").left()
            }
            else -> InternalServerError("Invalid result of createDatabase: $result").left()
        }.bind()
    }

    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> = either {
        val deferred = publishCommand(command).bind()

        // TODO: flatten this such that DatabaseCreationError is sibling of DatabaseCreated event, and only one `else` branch is need
        when(val result = deferred.await()) {
            is BatchTransacted -> result.right()
            is ErrorEnvelope -> when(val body = result.data){
                is BatchTransactionError -> body.left()
                else -> InternalServerError("Invalid result of createDatabase: $result").left()
            }
            else -> InternalServerError("Invalid result of createDatabase: $result").left()
        }.bind()
    }

    private suspend fun publishCommand(command: CommandEnvelope): Either<InternalServerError, CompletableDeferred<EventEnvelope>> {
        val deferred = CompletableDeferred<EventEnvelope>()
        inFlight[command.id] = deferred

        try {
            producer.publish(ProducerRecord(commandTopic, command.id, command))
        } catch (e: RuntimeException) {
            return InternalServerError("Unknown exception was thrown: $e").left()
        }

        return deferred.right()
    }
}

class KafkaService(
    producerConfig: ProducerConfig,
    consumerConfig: ConsumerConfig,
    brokerTimeout: Duration,
    commandTopic: String,
    eventTopic: String,

    streams: KafkaStreams,
    databaseStoreName: String,
    databaseNameStoreName: String,
): Service {
    override val databaseReadModel = DatabaseReadModelStore(
        streams.store(StoreQueryParameters.fromNameAndType(
                    databaseStoreName,
                    QueryableStoreTypes.keyValueStore()
                )
        ),
        streams.store(
            StoreQueryParameters.fromNameAndType(
                databaseNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        ),
    )
    override val commandBroker = KafkaCommandBroker(producerConfig, consumerConfig, brokerTimeout, commandTopic, eventTopic)
}