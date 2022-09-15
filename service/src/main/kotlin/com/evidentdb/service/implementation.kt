package com.evidentdb.service

import kotlin.concurrent.thread
import kotlin.coroutines.*
import java.util.concurrent.ConcurrentHashMap
import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain.*
import com.evidentdb.domain.CommandManager
import com.evidentdb.kafka.CommandEnvelopeSerde
import com.evidentdb.kafka.DatabaseReadModelStore
import com.evidentdb.kafka.EventEnvelopeSerde
import com.evidentdb.kafka.partitionByDatabase
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.UUIDDeserializer
import org.apache.kafka.common.serialization.UUIDSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.RuntimeException
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

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

class KafkaCommandManager(
    kafkaBootstrapServers: String,
    private val internalCommandsTopic: String,
    private val internalEventsTopic: String,
    private val meterRegistry: MeterRegistry,
) : CommandManager, AutoCloseable {
    private val running = AtomicBoolean(true)
    private val inFlight = ConcurrentHashMap<CommandId, CompletableDeferred<EventEnvelope>>()
    private val samples = ConcurrentHashMap<CommandId, Timer.Sample>()
    private val producer: Producer<CommandId, CommandEnvelope>
    private val producerMetrics: KafkaClientMetrics
    private val consumer: Consumer<EventId, EventEnvelope>
    private val consumerMetrics: KafkaClientMetrics

    companion object {
        private const val REQUEST_TIMEOUT = 3000L
        private val CONSUMER_POLL_INTERVAL = Duration.ofMillis(5000) // TODO: configurable?
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaCommandManager::class.java)
    }

    init {
        val producerConfig = Properties()
        producerConfig[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        // TODO: configurable CLIENT_ID
        producerConfig[ProducerConfig.PARTITIONER_CLASS_CONFIG] = DatabaseIdPartitioner::class.java
        producerConfig[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] = 30 * 1000 // TODO: configurable?
        producerConfig[ProducerConfig.LINGER_MS_CONFIG] = 0
        this.producer = KafkaProducer(
            producerConfig,
            UUIDSerializer(),
            CommandEnvelopeSerde.CommandEnvelopeSerializer()
        )
        this.producerMetrics = KafkaClientMetrics(producer)
        producerMetrics.bindTo(meterRegistry)

        val consumerConfig = Properties()
        consumerConfig[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        // TODO: configurable CLIENT_ID
        consumerConfig[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"
        consumerConfig[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumerConfig[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = false
        this.consumer = KafkaConsumer(
            consumerConfig,
            UUIDDeserializer(),
            EventEnvelopeSerde.EventEnvelopeDeserializer()
        )
        this.consumerMetrics = KafkaClientMetrics(consumer)
        consumerMetrics.bindTo(meterRegistry)
        thread {
            try {
                consumer.assign(consumer.listTopics()[internalEventsTopic]!!.map {
                    TopicPartition(it.topic(), it.partition())
                })
                while (running.get()) {
                    consumer.poll(CONSUMER_POLL_INTERVAL).forEach { record ->
                        LOGGER.info("Event received: ${record.key()}")
                        LOGGER.debug("Event data: ${record.value()}")
                        val commandId = record.value().commandId
                        inFlight.remove(commandId)?.complete(record.value())
                        samples.remove(commandId)?.stop(
                            meterRegistry.timer("in.flight.requests")
                        )
                    }
                }
            } catch (e: WakeupException) {
                if (running.get()) throw e
            } finally {
                consumer.close()
                consumerMetrics.close()
            }
        }
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

        // TODO: flatten this
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

        // TODO: flatten this such that DatabaseCreationError is sibling of DatabaseCreated event, and only one `else` branch is need
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

    override fun close() {
        running.set(false)
        consumer.wakeup()
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

class KafkaService(
    kafkaBootstrapServers: String,
    internalCommandsTopic: String,
    internalEventsTopic: String,

    // TODO: remove below, implement via gRPC client
    streams: KafkaStreams,
    databaseStoreName: String,
    meterRegistry: MeterRegistry,
): Service, AutoCloseable {
    override val databaseReadModel = DatabaseReadModelStore(
        streams.store(
            StoreQueryParameters.fromNameAndType(
                databaseStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        )
    )
    override val commandManager = KafkaCommandManager(
        kafkaBootstrapServers,
        internalCommandsTopic,
        internalEventsTopic,
        meterRegistry,
    )

    override fun close() {
        commandManager.close()
    }
}