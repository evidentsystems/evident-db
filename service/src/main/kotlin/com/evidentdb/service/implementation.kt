package com.evidentdb.service

import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain.*
import com.evidentdb.kafka.*
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.UUIDSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.*

private suspend inline fun <reified K: Any, reified V : Any> Producer<K, V>.publish(record: ProducerRecord<K, V>) =
    suspendCoroutine { continuation ->
        val callback = Callback { metadata, exception ->
            if (metadata == null) {
                continuation.resumeWithException(exception!!)
            } else {
                continuation.resume(metadata)
            }
        }
        this.send(record, callback)
    }

class DatabaseIdPartitioner: Partitioner {
    override fun partition(
        topic: String?,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster?
    ): Int =
        when(value) {
            is CommandEnvelope -> partitionByDatabase(value.database, cluster?.partitionCountForTopic(topic)!!)
            is EventEnvelope   -> partitionByDatabase(value.database, cluster?.partitionCountForTopic(topic)!!)
            else -> 0
        }

    override fun configure(configs: MutableMap<String, *>?) {}
    override fun close() {}
}

class KafkaProducerCommandManager(
    kafkaBootstrapServers: String,
    private val internalCommandsTopic: String,
    producerLingerMs: Int,
    private val meterRegistry: MeterRegistry,
    private val eventChannel: ReceiveChannel<EventEnvelope>
) : CommandManager, AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.Default)
    private val inFlight = ConcurrentHashMap<EnvelopeId, CompletableDeferred<EventEnvelope>>()
    private val samples = ConcurrentHashMap<EnvelopeId, Timer.Sample>()
    private val producer: Producer<EnvelopeId, CommandEnvelope>
    private val producerMetrics: KafkaClientMetrics

    companion object {
        private const val REQUEST_TIMEOUT = 3000L
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaProducerCommandManager::class.java)
    }

    init {
        val producerConfig = Properties()
        producerConfig[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        // TODO: configurable CLIENT_ID w/ Tenant
        producerConfig[ProducerConfig.PARTITIONER_CLASS_CONFIG] = DatabaseIdPartitioner::class.java
        producerConfig[ProducerConfig.LINGER_MS_CONFIG] = producerLingerMs
        this.producer = KafkaProducer(
            producerConfig,
            UUIDSerializer(),
            CommandEnvelopeSerde.CommandEnvelopeSerializer()
        )
        this.producerMetrics = KafkaClientMetrics(producer)
        producerMetrics.bindTo(meterRegistry)
    }

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> = either {
        val deferred = publishCommand(command).bind()

        // TODO: flatten this such that DatabaseCreationError is sibling of DatabaseCreated event, and only one `else` branch is need
        when(val result = withTimeoutOrNull(REQUEST_TIMEOUT) { deferred.await() }) {
            is DatabaseCreated -> result.right()
            is ErrorEnvelope -> when(val body = result.data){
                is DatabaseCreationError -> body.left()
                else -> InternalServerError("Invalid result of createDatabase: $result").left()
            }
            null -> InternalServerError("Timed out waiting for response").left()
            else -> InternalServerError("Invalid result of createDatabase: $result").left()
        }.bind()
    }

    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> = either {
        val deferred = publishCommand(command).bind()

        when(val result = withTimeoutOrNull(REQUEST_TIMEOUT) { deferred.await() }) {
            is DatabaseDeleted -> result.right()
            is ErrorEnvelope -> when(val body = result.data){
                is DatabaseDeletionError -> body.left()
                else -> InternalServerError("Invalid result of createDatabase: $result").left()
            }
            null -> InternalServerError("Timed out waiting for response").left()
            else -> InternalServerError("Invalid result of createDatabase: $result").left()
        }.bind()
    }

    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> = either {
        val deferred = publishCommand(command).bind()

        when(val result = withTimeoutOrNull(REQUEST_TIMEOUT) { deferred.await() }) {
            is BatchTransacted -> result.right()
            is ErrorEnvelope -> when(val body = result.data){
                is BatchTransactionError -> body.left()
                else -> InternalServerError("Invalid result of createDatabase: $result").left()
            }
            null -> InternalServerError("Timed out waiting for response").left()
            else -> InternalServerError("Invalid result of createDatabase: $result").left()
        }.bind()
    }

    fun start() = scope.launch {
        eventChannel.consumeEach { event ->
            LOGGER.info("Event received on response channel: ${event.id}")
            LOGGER.debug("Received response event data: $event")
            val commandId = event.commandId
            inFlight.remove(commandId)?.complete(event)
            samples.remove(commandId)?.stop(
                meterRegistry.timer("in.flight.requests")
            )
        }
    }

    override fun close() {
        scope.cancel()
        producer.close(Duration.ofMillis(10000))
        producerMetrics.close()
    }

    private suspend fun publishCommand(command: CommandEnvelope): Either<InternalServerError, CompletableDeferred<EventEnvelope>> {
        val deferred = CompletableDeferred<EventEnvelope>()
        inFlight[command.id] = deferred
        samples[command.id] = Timer.start(meterRegistry)

        LOGGER.info("Sending command: ${command.id}...")
        LOGGER.debug("Command data: $command")
        try {
            val metadata = producer.publish(ProducerRecord(internalCommandsTopic, command.id, command))
            LOGGER.info("...sent ${command.id}")
            LOGGER.debug("Command ${command.id} record metadata: ${metadata.topic()}-${metadata.partition()}" +
                    "@${metadata.offset()}" +
                    ", timestamp: ${metadata.timestamp()}" +
                    ", value size: ${metadata.serializedValueSize()}"
            )
        } catch (e: RuntimeException) {
            return InternalServerError("Unknown exception was thrown: $e").left()
        }

        return deferred.right()
    }
}

class KafkaCommandService(
    kafkaBootstrapServers: String,
    internalCommandsTopic: String,
    producerLingerMs: Int,
    eventChannel: ReceiveChannel<EventEnvelope>,
    meterRegistry: MeterRegistry,
): CommandService, AutoCloseable {
    override val commandManager = KafkaProducerCommandManager(
        kafkaBootstrapServers,
        internalCommandsTopic,
        producerLingerMs,
        meterRegistry,
        eventChannel,
    )

    fun start() {
        commandManager.start()
    }

    override fun close() {
        commandManager.close()
    }
}

class KafkaQueryService(
    kafkaStreams: KafkaStreams,
    databaseStoreName: String,
    logStoreName: String,
    batchStoreName: String,
    streamStoreName: String,
    eventStoreName: String,
): QueryService {
    override val databaseReadModel: DatabaseReadModelStore
    override val batchReadModel: BatchReadOnlyStore
    override val streamReadModel: StreamReadOnlyStore
    override val eventReadModel: EventReadOnlyStore

    init {
        val logStore: DatabaseLogReadOnlyKeyValueStore = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                logStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        )

        databaseReadModel = DatabaseReadModelStore(
            kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                    databaseStoreName,
                    QueryableStoreTypes.keyValueStore()
                )
            ),
            logStore
        )

        val eventKeyValueStore: EventReadOnlyKeyValueStore = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                eventStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        )

        batchReadModel = BatchReadOnlyStore(
            kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                    batchStoreName,
                    QueryableStoreTypes.keyValueStore()
                )
            ),
            logStore,
            eventKeyValueStore,
        )

        streamReadModel = StreamReadOnlyStore(
            kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                    streamStoreName,
                    QueryableStoreTypes.keyValueStore()
                )
            )
        )

        eventReadModel = EventReadOnlyStore(eventKeyValueStore)
    }
}
