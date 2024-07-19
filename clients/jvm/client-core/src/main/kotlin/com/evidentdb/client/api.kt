package com.evidentdb.client

import com.evidentdb.client.cloudevents.RecordedTimeExtension
import com.evidentdb.client.cloudevents.SequenceExtension
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.CloudEventExtension
import io.cloudevents.core.builder.CloudEventBuilder
import java.net.URI

interface ClientHelpers {
    // These need to be registered before any event processing is done
    fun init() {
        SequenceExtension.register()
        RecordedTimeExtension.register()
    }

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

interface Shutdown {
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