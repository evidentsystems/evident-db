package io.cloudevents.protobuf

import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder.fromSpecVersion
import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent

fun cloudEventFromProto(message: ProtoCloudEvent): CloudEvent =
    ProtoDeserializer(message).read(::fromSpecVersion)

fun CloudEvent.toProto(): ProtoCloudEvent =
    ProtoSerializer.toProto(this)