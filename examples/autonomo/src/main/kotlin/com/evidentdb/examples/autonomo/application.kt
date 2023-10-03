package com.evidentdb.examples.autonomo

import com.evidentdb.client.EvidentDbKt
import com.evidentdb.client.EvidentDb
import com.evidentdb.client.kotlin.Connection
import com.evidentdb.examples.autonomo.domain.*
import io.grpc.ManagedChannelBuilder
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.Micronaut.*
import jakarta.inject.Singleton
import kotlinx.coroutines.runBlocking
import java.util.*

fun main(args: Array<String>) {
	run(*args)
}

@Factory
class Beans {
	@Singleton
	fun service(conn: Connection): CommandService =
		EvidentDbService(conn)

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
}

@Bean
class VehicleEventSourcingDecisionExecutor(
	override val repository: IEventRepository<VehicleEvent>,
	override val domainLogic: Decide<VehicleCommand, Vehicle, VehicleEvent>
): EventSourcingDecisionExecutor<VehicleCommand, Vehicle, VehicleEvent>

@Bean
class RideEventSourcingDecisionExecutor(
	override val repository: IEventRepository<RideEvent>,
	override val domainLogic: Decide<RideCommand, Ride, RideEvent>
): EventSourcingDecisionExecutor<RideCommand, Ride, RideEvent>
