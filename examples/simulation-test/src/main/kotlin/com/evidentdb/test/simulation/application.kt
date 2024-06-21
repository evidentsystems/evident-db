package com.evidentdb.test.simulation

import io.micronaut.runtime.Micronaut.run
import com.evidentdb.client.EvidentDb
import com.evidentdb.client.EvidentDbKt
import com.evidentdb.client.kotlin.kotlinClient
import io.grpc.ManagedChannelBuilder
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.serde.ObjectMapper
import jakarta.inject.Singleton
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

fun main(args: Array<String>) {
	run(*args)
}

private const val SUCCESS_FAILURE_COUNTS_BY_DB_STORE = "SuccessFailureCountsByDb"

@Factory
class Configuration {
	private val databases = ConcurrentHashMap<String, Unit>(10)

	companion object {
		private val LOGGER = LoggerFactory.getLogger(
			"com.evidentdb.test.simulation.Configuration"
		)
	}

	@Singleton
	fun managedChannelBuilder(
		@Value("\${evidentdb.host}")
		host: String,
		@Value("\${evidentdb.port}")
		port: Int,
		@Value("\${evidentdb.usePlaintext}")
		usePlaintext: Boolean = false
	): ManagedChannelBuilder<*> {
		LOGGER.info(
			"Initializing ManagedChannelBuilder with host: {}, port: {}, plaintext? {}",
			host, port, usePlaintext
		)
		val builder = ManagedChannelBuilder.forAddress(host, port)
		if (usePlaintext) {
			builder.usePlaintext()
		} else {
			builder.useTransportSecurity()
		}
		LOGGER.info("...Initialized ManagedChannelBuilder: {}", builder)
		return builder
	}

	@Singleton
	@Bean(preDestroy = "shutdown")
	fun evidentDbClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDbKt =
		EvidentDb.kotlinClient(channelBuilder)

	@Singleton
	fun topology(
		@Value("\${simulation.input.topic}")
		batchesInputTopic: String,
		@Value("\${simulation.output.topic}")
		resultsOutputTopic: String,
		objectMapper: ObjectMapper,
		evidentDb: EvidentDbKt
	): Topology {
		LOGGER.info(
			"Initializing topology with input topic: {} and output topic: {}",
			batchesInputTopic, resultsOutputTopic
		)
		val builder = StreamsBuilder()
		val results = builder.stream(
			batchesInputTopic,
			Consumed.with(Serdes.String(), MicronautSerde(
				objectMapper,
				BatchInput::class.java)
			)
		).mapValues { batch ->
			val start = Instant.now()
			if (!databases.containsKey(batch.database)) {
				println("Cache miss, can't find ${batch.database} in cache: ${databases.keys()}")
				if (evidentDb.createDatabase(batch.database)) {
					databases[batch.database] = Unit
				}
			}
			val conn = evidentDb.connectDatabase(batch.database)
			TransactionResultOutput.fromBatchInput(batch, conn, start)
		}

		results.groupBy({ k, v ->
			val result = when(v) {
				is TransactionResultOutput.Success -> "success"
				is TransactionResultOutput.Failure -> "failure"
			}
			"$k-$result"
		}, Grouped.with(
			Serdes.String(),
			MicronautSerde(
				objectMapper,
				TransactionResultOutput::class.java
			))
		).count(
			Materialized
				.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>(SUCCESS_FAILURE_COUNTS_BY_DB_STORE)
				.withStoreType(Materialized.StoreType.IN_MEMORY)
		)

		results.to(
			resultsOutputTopic,
			Produced.with(
				Serdes.String(),
				MicronautSerde(objectMapper, TransactionResultOutput::class.java)
			)
		)
		LOGGER.info("...Initialized topology: {}", builder)
		return builder.build()
	}

	@Context
	@Bean(preDestroy = "close")
	fun streams(
		@Value("\${micronaut.application.name}")
		applicationName: String,
		@Value("\${kafka.bootstrap.servers}")
		bootstrapServers: String,
		topology: Topology
	): KafkaStreams {
		LOGGER.info("Initializing KafkaStreams")
		val config = mutableMapOf(
			StreamsConfig.APPLICATION_ID_CONFIG to applicationName,
			StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
		)
		val kafkaStreams = KafkaStreams(topology, StreamsConfig(config))

		kafkaStreams.start()
		LOGGER.info("...Initialized KafkaStreams: {}", kafkaStreams)
		return kafkaStreams
	}
}

@Controller("/metrics")
class MetricsController(private val kafkaStreams: KafkaStreams) {
	private val logger = LoggerFactory.getLogger(MetricsController::class.java)

	@Get("/{database}")
	fun metrics(database: String): Map<String, Long> {
		val store: ReadOnlyKeyValueStore<String, Long> = kafkaStreams
			.store(StoreQueryParameters.fromNameAndType(
				SUCCESS_FAILURE_COUNTS_BY_DB_STORE,
				QueryableStoreTypes.keyValueStore()
			))
		val successKey = "${database}-success"
		val successCount = store.get(successKey)
		val failureKey = "${database}-failure"
		val failureCount = store.get(failureKey)
		return mapOf(
			successKey to successCount,
			failureKey to failureCount,
		)
	}
}
