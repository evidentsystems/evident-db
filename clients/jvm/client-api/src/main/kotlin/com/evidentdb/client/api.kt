package com.evidentdb.client

import com.evidentdb.client.cloudevents.RecordedTimeExtension
import com.evidentdb.client.cloudevents.SequenceExtension
import com.evidentdb.client.java.Connection
import com.evidentdb.client.kotlin.Connection as ConnectionKt
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.CloudEventExtension
import io.cloudevents.core.builder.CloudEventBuilder
import kotlinx.coroutines.flow.Flow
import java.net.URI

interface EvidentDb: Lifecycle {
    /**
     * Synchronously creates a database, which serves as the unit of total
     * event ordering in an event-sourcing system.
     *
     * @param name The name of the database to create, must match
     *  """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$""".
     * @return `true` if database was created, `false` otherwise.
     */
    fun createDatabase(name: DatabaseName): Boolean

    /**
     * Synchronously deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     */
    fun deleteDatabase(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases as an iterator.
     *
     * This iterator coordinates with the server to provide back-pressure. Callers
     * must [CloseableIterator.close] after using the returned iterator, whether the
     * iterator is consumed to completion or not.
     *
     * @returns a [CloseableIterator] of [Database].
     */
    fun fetchCatalog(): CloseableIterator<DatabaseName>

    /**
     * Returns a connection to a specific database. This method caches,
     * so subsequent calls for the same database name will return
     * the same connection.
     *
     * @param name the database name.
     * @return the [Connection], possibly cached.
     */
    fun connectDatabase(name: DatabaseName): Connection

    companion object {
        // These need to be registered before any event processing is done
        init {
            SequenceExtension.register()
            RecordedTimeExtension.register()
        }

        @JvmStatic
        fun eventBuilder(
            streamName: StreamName,
            eventId: String,
            eventType: String,
            subject: String? = null,
            data: CloudEventData? = null,
            dataContentType: String? = null,
            dataSchema: URI? = null,
            extensions: List<CloudEventExtension> = listOf(),
        ): CloudEventBuilder {
            val builder = CloudEventBuilder.v1()
                .withId(eventId)
                .withSource(URI(streamName))
                .withType(eventType)
                .withData(data)
                .withDataContentType(dataContentType)
                .withDataSchema(dataSchema)
                .withSubject(subject)
            for (extension in extensions)
                builder.withExtension(extension)
            return builder
        }

        @JvmStatic
        fun event(
            streamName: StreamName,
            eventId: String,
            eventType: String,
            subject: String? = null,
            data: CloudEventData? = null,
            dataContentType: String? = null,
            dataSchema: URI? = null,
            extensions: List<CloudEventExtension> = listOf(),
        ): CloudEvent = eventBuilder(
            streamName, eventId, eventType, subject, data, dataContentType, dataSchema, extensions
        ).build()
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
     */
    suspend fun createDatabaseAsync(name: DatabaseName): Boolean

    /**
     * Deletes a database.
     *
     * @param name The name of the database to delete.
     * @return `true` if database was deleted, `false` otherwise.
     */
    suspend fun deleteDatabaseAsync(name: DatabaseName): Boolean

    /**
     * Returns the catalog of all available databases as a [Flow].
     *
     * @returns a [Flow] of [Database].
     */
    fun fetchCatalogAsync(): Flow<DatabaseName>

    /**
     * Returns a connection to a specific database. This method caches,
     * so subsequent calls for the same database name will return
     * the same connection.
     *
     * @param name the database name.
     * @return the [ConnectionKt], possibly cached.
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
