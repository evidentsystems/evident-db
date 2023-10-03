package io.cloudevents.protobuf

import com.google.protobuf.Message
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.core.builder.CloudEventBuilder.fromSpecVersion
import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent

// ***** CloudEvents themselves (the envelope) *****

fun ProtoCloudEvent.toDomain(): CloudEvent =
    ProtoDeserializer(this).read(::fromSpecVersion)

fun CloudEvent.toProto(): ProtoCloudEvent =
    ProtoSerializer.toProto(this)

// ***** CloudEvent data (the message) *****

fun Message.toCloudEventData(): CloudEventData =
    ProtoCloudEventData.wrap(this)
