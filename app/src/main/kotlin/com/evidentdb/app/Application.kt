package com.evidentdb.app

import com.evidentdb.domain.*
import com.evidentdb.service.EvidentDbEndpoint
import com.evidentdb.service.KafkaCommandService
import com.evidentdb.service.KafkaQueryService
import com.evidentdb.transactor.TransactorTopology
import io.grpc.BindableService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.health.HealthStatus
import io.micronaut.management.endpoint.health.HealthEndpoint
import io.micronaut.management.health.indicator.AbstractHealthIndicator
import io.micronaut.management.health.indicator.annotation.Liveness
import io.micronaut.management.health.indicator.annotation.Readiness
import io.micronaut.runtime.Micronaut.build
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Inject
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap

const val BUFFER_SIZE = 1000

fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("com.evidentdb.app")
		.start()
}

@Factory
class Configuration {
	companion object {
		private val LOGGER = LoggerFactory.getLogger(InternalEventListener::class.java)
	}

	@Inject
	lateinit var transactorTopologyRunner: TransactorTopologyRunner

	@Inject
	lateinit var transactorHealthIndicator: TransactorHealthIndicator

	@Bean(preDestroy = "close")
	@Singleton
	fun commandService(
		@Value("\${kafka.bootstrap.servers}")
		kafkaBootstrapServers: String,
		@Value("\${kafka.topics.internal-commands.name}")
		internalCommandsTopic: String,
		@Value("\${kafka.producers.default.linger.ms}")
		producerLingerMs: Int,
		commandResponseChannel: Channel<EventEnvelope>,
		meterRegistry: MeterRegistry,
	): KafkaCommandService {
		val service = KafkaCommandService(
			kafkaBootstrapServers,
			internalCommandsTopic,
			producerLingerMs,
			commandResponseChannel,
			meterRegistry,
		)
		service.start()
		return service
	}

	@Singleton
	fun databaseRevisionFlow(listener: InternalEventListener): SharedFlow<Database> {
		val mutableSharedFlow = MutableSharedFlow<Database>(0, extraBufferCapacity = BUFFER_SIZE)
		listener.registerListener("database-revisions") { event ->
			databaseRevisionFromEvent(event)?.let {
				mutableSharedFlow.emit(it)
			}
		}
		return mutableSharedFlow.asSharedFlow()
	}

	@Singleton
	fun commandResponseChannel(listener: InternalEventListener): Channel<EventEnvelope> {
		val channel = Channel<EventEnvelope>(BUFFER_SIZE)
		listener.registerListener("command-responses") { event ->
			channel.send(event)
		}
		return channel
	}

	@Singleton
	fun queryService(transactorTopologyRunner: TransactorTopologyRunner): KafkaQueryService =
		KafkaQueryService(
			transactorTopologyRunner.streams,
			TransactorTopology.DATABASE_STORE,
			TransactorTopology.DATABASE_LOG_STORE,
			TransactorTopology.BATCH_STORE,
			TransactorTopology.STREAM_STORE,
			TransactorTopology.EVENT_STORE,
		)

	@Bean
	fun endpoint(
		commandService: CommandService,
		queryService: QueryService,
		databaseRevisionFlow: SharedFlow<Database>,
	): BindableService =
		EvidentDbEndpoint(
			commandService,
			queryService,
			databaseRevisionFlow
		)
}

@Singleton
class TransactorTopologyRunner(
	@Value("\${evidentdb.transactor.state.dir}")
	stateDir: String,
	@Value("\${kafka.topics.internal-commands.name}")
	internalCommandsTopic: String,
	@Value("\${kafka.topics.internal-commands.partitions}")
	internalCommandsPartitions: Int,
	@Value("\${kafka.topics.internal-events.name}")
	internalEventsTopic: String,
	@Value("\${micronaut.application.name}")
	applicationId: String,
	@Value("\${kafka.bootstrap.servers}")
	kafkaBootstrapServers: String,
	@Value("\${kafka.streams.producer.linger.ms}")
	producerLingerMs: Int,
	meterRegistry: MeterRegistry,
) {
	val streams: KafkaStreams
	private val metrics: KafkaStreamsMetrics

	init {
		val config = Properties()
		val appId = "$applicationId-evidentdb-transactor"
		config[StreamsConfig.APPLICATION_ID_CONFIG] = appId
		config[StreamsConfig.STATE_DIR_CONFIG] = stateDir
		config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
		config[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = "at_least_once" // "exactly_once_v2"
		config[StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG] = StreamsConfig.OPTIMIZE
		config[StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG)] = "all"
		config[StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG)] = producerLingerMs
		config[StreamsConfig.RETRY_BACKOFF_MS_CONFIG] = 0
		config[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = internalCommandsPartitions
		// TODO: standby replicas for query high-availability
		// config[StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG] = 1
//      On broker:
//		log.flush.interval.messages=1
//		log.flush.interval.ms=0

		val topology = TransactorTopology.build(
			internalCommandsTopic,
			internalEventsTopic,
			meterRegistry,
		)

		this.streams = KafkaStreams(topology, config)
		this.metrics = KafkaStreamsMetrics(streams)
		metrics.bindTo(meterRegistry)
	}

	@PostConstruct
	fun start() {
		streams.start()
	}

	@PreDestroy
	fun stop() {
		streams.close(Duration.ofMillis(10000))
		metrics.close()
	}
}

// TODO: configurable CLIENT_ID/GROUP_ID w/ Tenant
@KafkaListener(
	groupId = "\${micronaut.application.name}-internal-event-listener",
	uniqueGroupId = true,
	isolation = IsolationLevel.READ_COMMITTED,
	offsetStrategy = OffsetStrategy.DISABLED,
	offsetReset = OffsetReset.LATEST,
	properties = [Property(name = ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, value = "false")]
)
class InternalEventListener {
	companion object {
		private val LOGGER = LoggerFactory.getLogger(InternalEventListener::class.java)
	}
	private val scope = CoroutineScope(Dispatchers.Default)
	private val listeners = ConcurrentHashMap<String, suspend (EventEnvelope) -> Unit>(2)

	fun registerListener(key: String, listener: suspend (EventEnvelope) -> Unit) {
		listeners[key] = listener
	}

	fun unregisterListener(key: String) {
		listeners.remove(key)
	}

	@Topic("\${kafka.topics.internal-events.name}")
	fun receive(event: EventEnvelope) {
		LOGGER.info("Invoking InternalEventListener for event ${event.id}")
		LOGGER.debug("KafkaListener event data $event, listeners $listeners")
		for ((key, listener) in listeners) {
			scope.launch {
				listener.invoke(event)
			}
		}
	}

	@PreDestroy
	fun close() {
		scope.cancel()
	}
}

@Singleton
@Liveness
@Readiness
@Requires(beans = [HealthEndpoint::class])
class TransactorHealthIndicator(
	private val runner: TransactorTopologyRunner,
): AbstractHealthIndicator<KafkaStreams.State>() {
	companion object {
		const val NAME = "transactor"
	}

	override fun getName(): String = NAME

	override fun getHealthInformation(): KafkaStreams.State {
		val state = runner.streams.state()
		when(state) {
			KafkaStreams.State.RUNNING -> this.healthStatus = HealthStatus.UP
			else -> this.healthStatus = HealthStatus.DOWN
		}
		return state
	}
}