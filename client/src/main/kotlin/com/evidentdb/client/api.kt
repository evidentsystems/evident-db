package com.evidentdb.client

import com.evidentdb.client.java.Client as JavaClient
import com.evidentdb.client.java.EvidentDB as JavaClientImpl
import com.evidentdb.client.kotlin.Client as KotlinClient
import com.evidentdb.client.kotlin.EvidentDB as KotlinClientImpl

import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.CloudEventExtension
import io.cloudevents.core.builder.CloudEventBuilder
import io.grpc.ManagedChannelBuilder
import java.net.URI
import java.util.*

object EvidentDB {
    @JvmStatic
    fun javaClient(channelBuilder: ManagedChannelBuilder<*>): JavaClient =
        JavaClientImpl(channelBuilder)

    @JvmStatic
    fun kotlinClient(channelBuilder: ManagedChannelBuilder<*>): KotlinClient =
        KotlinClientImpl(channelBuilder)

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
        eventId: String? = null,
        data: CloudEventData? = null,
        dataContentType: String? = null,
        dataSchema: URI? = null,
        subject: String? = null,
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

interface GrpcLifecycle {
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