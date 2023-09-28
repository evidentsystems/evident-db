package com.evidentdb.client

import com.evidentdb.client.java.Connection
import com.evidentdb.client.java.GrpcClient
import com.evidentdb.client.kotlin.GrpcClient as GrpcClientKt
import com.evidentdb.client.kotlin.Connection as ConnectionKt

import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.CloudEventExtension
import io.cloudevents.core.builder.CloudEventBuilder
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.flow.Flow
import java.net.URI
import java.util.*

interface EvidentDb: Lifecycle {
    /**
     * Synchronously creates a database, which serves as the unit of total
     * event ordering in an event-sourcing system.
     *
     * @param name The name of the database to create, must match
     *  """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$""".
     * @return `true` if database was created, `false` otherwise.
     * @throws InvalidDatabaseNameError
     * @throws DatabaseNameAlreadyExistsError
     * @throws InternalServerError
     * @throws SerializationError
     */
    fun createDatabase(name: DatabaseName): Boolean

    /**
     * Synchronously deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     * @throws InvalidDatabaseNameError
     * @throws DatabaseNotFoundError
     * @throws InternalServerError
     * @throws SerializationError
     */
    fun deleteDatabase(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases as an iterator.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @returns a [CloseableIterator] of [DatabaseSummary].
     */
    fun fetchCatalog(): CloseableIterator<DatabaseSummary>

    /**
     * Returns a connection to a specific database. This method caches,
     * so subsequent calls for the same database name will return
     * the same connection.
     *
     * @param name the database name.
     * @return the [Connection], possibly cached.
     * @throws DatabaseNotFoundError
     * @throws SerializationError
     */
    fun connectDatabase(name: DatabaseName): Connection

    companion object {
        @JvmStatic
        fun javaClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDb =
            GrpcClient(channelBuilder)

        @JvmStatic
        fun kotlinClient(channelBuilder: ManagedChannelBuilder<*>): EvidentDbKt =
            GrpcClientKt(channelBuilder)

        @JvmStatic
        fun eventProposal(
            event: CloudEvent,
            streamName: StreamName,
            streamState: ProposedEventStreamState = StreamState.Any,
        ) = EventProposal(
            event,
            streamName,
            streamState,
        )

        @JvmStatic
        fun eventProposal(
            eventType: String,
            streamName: StreamName,
            streamState: ProposedEventStreamState = StreamState.Any,
            subject: String? = null,
            eventId: String? = null,
            data: CloudEventData? = null,
            dataContentType: String? = null,
            dataSchema: URI? = null,
            extensions: List<CloudEventExtension> = listOf(),
        ): EventProposal {
            val builder = CloudEventBuilder.v1()
                .withId(eventId ?: UUID.randomUUID().toString())
                .withSource(URI("edb:client"))
                .withType(eventType)
                .withData(data)
                .withDataContentType(dataContentType)
                .withDataSchema(dataSchema)
                .withSubject(subject)
            for (extension in extensions)
                builder.withExtension(extension)
            return EventProposal(
                builder.build(),
                streamName,
                streamState,
            )
        }
    }
}

interface EvidentDbKt: EvidentDb {
    /**
     * Creates a database, which serves as the unit of total
     * event ordering in an event-sourcing system.
     *
     * @param name The name of the database to create, must match
     *  """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$""".
     * @return `true` if database was created, `false` otherwise.
     * @throws InvalidDatabaseNameError
     * @throws DatabaseNameAlreadyExistsError
     * @throws InternalServerError
     * @throws SerializationError
     */
    suspend fun createDatabaseAsync(name: DatabaseName): Boolean

    /**
     * Deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     * @throws InvalidDatabaseNameError
     * @throws DatabaseNotFoundError
     * @throws InternalServerError
     * @throws SerializationError
     */
    suspend fun deleteDatabaseAsync(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases as a [Flow].
     *
     * @returns a [Flow] of [DatabaseSummary].
     */
    fun fetchCatalogAsync(): Flow<DatabaseSummary>

    /**
     * Returns a connection to a specific database. This method caches,
     * so subsequent calls for the same database name will return
     * the same connection.
     *
     * @param name the database name.
     * @return the [ConnectionKt], possibly cached.
     * @throws DatabaseNotFoundError
     * @throws SerializationError
     */
    override fun connectDatabase(name: DatabaseName): ConnectionKt
}

//// Util

interface Lifecycle {
    /**
     * Shuts down this resource while awaiting any in-flight requests to
     * complete, and cleans up local state.
     * */
    fun shutdown()

    /**
     * Shuts down this connection immediately, not awaiting in-flight requests to
     * complete, and cleans up local state.
     * */
    fun shutdownNow()
}

interface CloseableIterator<T>: Iterator<T>, AutoCloseable
