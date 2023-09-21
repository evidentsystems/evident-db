package com.evidentdb.examples.autonomo

import com.evidentdb.client.kotlin.Client
import com.evidentdb.client.EvidentDB
import com.evidentdb.client.kotlin.Connection
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
		client: Client,
	): Connection = runBlocking {
		client.createDatabase(databaseName)
		client.connectDatabase(databaseName)
	}

	@Singleton
	@Bean(preDestroy = "shutdown")
	fun evidentDbClient(
		@Value("\${evident-db.host}")
		host: String,
		@Value("\${evident-db.port}")
		port: Int,
	): Client {
		val builder = ManagedChannelBuilder
			.forAddress(host, port)
			.usePlaintext()
		return EvidentDB.kotlinClient(builder)
	}
}
