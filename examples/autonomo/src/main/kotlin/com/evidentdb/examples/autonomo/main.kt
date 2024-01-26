package com.evidentdb.examples.autonomo

import com.evidentdb.client.EvidentDb
import com.evidentdb.client.EvidentDbKt
import com.evidentdb.client.kotlin.Connection
import com.evidentdb.examples.autonomo.adapters.EvidentDbRideService
import com.evidentdb.examples.autonomo.adapters.EvidentDbVehicleService
import io.grpc.ManagedChannelBuilder
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.Micronaut.run
import jakarta.inject.Singleton
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) {
	run(*args)
}

@Factory
class Beans {
	@Singleton
	@Bean(preDestroy = "shutdown")
	fun evidentDbConnection(
		@Value("\${evident-db.database-name}")
		databaseName: String,
		client: EvidentDbKt,
	): Connection = runBlocking {
		client.createDatabaseAsync(databaseName)
		client.connectDatabase(databaseName)
	}

	@Singleton
	@Bean(preDestroy = "shutdown")
	fun evidentDbClient(
		@Value("\${evident-db.host}")
		host: String,
		@Value("\${evident-db.port}")
		port: Int,
	): EvidentDbKt {
		val builder = ManagedChannelBuilder
			.forAddress(host, port)
			.usePlaintext()
		return EvidentDb.kotlinClient(builder)
	}

    @Singleton
    fun rideService(conn: Connection) = EvidentDbRideService(conn)

    @Singleton
    fun vehicleService(conn: Connection) = EvidentDbVehicleService(conn)
}
