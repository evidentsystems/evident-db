package io.cloudevents.protobuf

import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder.fromSpecVersion

fun cloudEventFromProto(message: ProtoCloudEvent): CloudEvent =
    ProtoDeserializer(message).read(::fromSpecVersion)

fun CloudEvent.toProto(): ProtoCloudEvent =
    ProtoSerializer.toProto(this)