package com.evidentdb.app

import com.evidentdb.domain.CommandService
import com.evidentdb.domain.QueryService
import com.evidentdb.service.EvidentDbEndpoint
import com.evidentdb.service.KafkaCommandService
import com.evidentdb.service.KafkaQueryService
import com.evidentdb.transactor.TransactorTopology
import io.grpc.BindableService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
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
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.time.Duration
import java.util.*

fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("com.evidentdb.app")
		.start()
}

@Factory
class Configuration {
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
		@Value("\${kafka.topics.internal-events.name}")
		internalEventsTopic: String,
		@Value("\${kafka.producer.linger.ms}")
		producerLingerMs: Int,
		meterRegistry: MeterRegistry,
	): KafkaCommandService =
		KafkaCommandService(
			kafkaBootstrapServers,
			internalCommandsTopic,
			internalEventsTopic,
			producerLingerMs,
			meterRegistry,
		)

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
	): BindableService =
		EvidentDbEndpoint(commandService, queryService)
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