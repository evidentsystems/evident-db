package com.evidentdb.app

import com.evidentdb.domain.Service
import com.evidentdb.service.EvidentDbEndpoint
import com.evidentdb.service.KafkaService
import com.evidentdb.transactor.TransactorTopology
import io.grpc.BindableService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.Micronaut.*
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
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

	@Bean(preDestroy = "close")
	@Singleton
	fun service(
		@Value("\${kafka.bootstrap.servers}")
		kafkaBootstrapServers: String,
		@Value("\${kafka.topics.internal-commands.name}")
		internalCommandsTopic: String,
		@Value("\${kafka.topics.internal-events.name}")
		internalEventsTopic: String,
		@Value("\${kafka.producer.linger.ms}")
		producerLingerMs: Int,
		meterRegistry: MeterRegistry,
	): KafkaService =
		KafkaService(
			kafkaBootstrapServers,
			internalCommandsTopic,
			internalEventsTopic,
			transactorTopologyRunner.streams,
			TransactorTopology.DATABASE_STORE,
			producerLingerMs,
			meterRegistry,
		)

	@Bean
	fun endpoint(service: Service): BindableService = EvidentDbEndpoint(service)
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
