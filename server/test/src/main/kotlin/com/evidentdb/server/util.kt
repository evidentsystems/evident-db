package com.evidentdb.server

import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import java.net.URI
import java.time.OffsetDateTime
import java.util.*

fun buildTestEvent(type: String, attributes: Map<String, Any>? = null): CloudEvent {
    val builder = CloudEventBuilder.v1()
        .withType(type)
        .withSource(URI("evidentdb://test-event"))
        .withId(UUID.randomUUID().toString())
    if (attributes != null) {
        for ((k, v) in attributes)
            when(v) {
                is String -> builder.withContextAttribute(k,v)
                is URI -> builder.withContextAttribute(k,v)
                is OffsetDateTime -> builder.withContextAttribute(k,v)
                is Number -> builder.withContextAttribute(k,v)
                is Int -> builder.withContextAttribute(k,v)
                is Boolean -> builder.withContextAttribute(k,v)
                is ByteArray -> builder.withContextAttribute(k,v)
                else -> throw IllegalArgumentException("$v is not a legal CloudEvent attribute value for key $k")
            }
    }
    return builder.build()
}
