package com.evidentdb.app

import com.evidentdb.domain.Service
import com.evidentdb.service.EvidentDbEndpoint
import com.evidentdb.service.KafkaService
import com.evidentdb.service.ServiceReadModelTopology
import com.evidentdb.transactor.TransactorTopology
import io.grpc.BindableService
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.Micronaut.*
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.time.Duration
import java.util.*

fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("com.evidentdb")
		.start()
}

@Factory
class Configuration {
	@Bean(preDestroy = "close")
	@Singleton
	fun service(
		readModelTopologyRunner: ServiceReadModelTopologyRunner,
		@Value("\${kafka.bootstrap.servers}")
		kafkaBootstrapServers: String,
		@Value("\${kafka.topics.internal-commands.name}")
		internalCommandsTopic: String,
		@Value("\${kafka.topics.internal-events.name}")
		internalEventsTopic: String,
		): KafkaService =
		KafkaService(
			kafkaBootstrapServers,
			internalCommandsTopic,
			internalEventsTopic,
			readModelTopologyRunner.streams,
			ServiceReadModelTopology.DATABASE_STORE,
			ServiceReadModelTopology.DATABASE_NAME_LOOKUP,
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
	@Value("\${kafka.topics.internal-events.name}")
	internalEventsTopic: String,
	@Value("\${kafka.topics.databases.name}")
	databasesTopic: String,
	@Value("\${kafka.topics.database-names.name}")
	databaseNamesTopic: String,
	@Value("\${kafka.topics.batches.name}")
	batchesTopic: String,
	@Value("\${kafka.topics.streams.name}")
	streamsTopic: String,
	@Value("\${kafka.topics.events.name}")
	eventsTopic: String,
	@Value("\${micronaut.application.name}")
	applicationId: String,
	@Value("\${kafka.bootstrap.servers}")
	kafkaBootstrapServers: String,
) {
	private val streams: KafkaStreams

	init {
		val config = Properties()
		config[StreamsConfig.APPLICATION_ID_CONFIG] = "$applicationId-transactor"
		config[StreamsConfig.STATE_DIR_CONFIG] = stateDir
		config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
		config[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = "exactly_once_v2"
		// TODO: standby replicas for query availability

		val topology = TransactorTopology.build(
			internalCommandsTopic,
			internalEventsTopic,
			databasesTopic,
			databaseNamesTopic,
			batchesTopic,
			streamsTopic,
			eventsTopic
		)

		this.streams = KafkaStreams(topology, config)
	}

	@PostConstruct
	fun start() {
		streams.start()
	}

	@PreDestroy
	fun stop() {
		streams.close(Duration.ofMillis(10000))
	}
}

@Singleton
class ServiceReadModelTopologyRunner(
	@Value("\${evidentdb.service.state.dir}")
	stateDir: String,
	@Value("\${kafka.topics.databases.name}")
	databasesTopic: String,
	@Value("\${kafka.topics.database-names.name}")
	databaseNamesTopic: String,
	@Value("\${kafka.topics.batches.name}")
	batchesTopic: String,
	@Value("\${kafka.topics.streams.name}")
	streamsTopic: String,
	@Value("\${kafka.topics.events.name}")
	eventsTopic: String,
	@Value("\${micronaut.application.name}")
	applicationId: String,
	@Value("\${kafka.bootstrap.servers}")
	kafkaBootstrapServers: String,
) {
	val streams: KafkaStreams

	init {
		val config = Properties()
		config[StreamsConfig.APPLICATION_ID_CONFIG] = "$applicationId-service-read-model"
		config[StreamsConfig.STATE_DIR_CONFIG] = stateDir
		config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
		// TODO: standby replicas for query availability

		val topology = ServiceReadModelTopology.build(
			databasesTopic,
			databaseNamesTopic,
			batchesTopic,
			streamsTopic,
			eventsTopic
		)

		this.streams = KafkaStreams(topology, config)
	}

	@PostConstruct
	fun start() {
		streams.start()
	}

	@PreDestroy
	fun stop() {
		streams.close(Duration.ofMillis(10000))
	}
}