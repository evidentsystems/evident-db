package io.cloudevents.protobuf

import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder.fromSpecVersion
import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent

fun ProtoCloudEvent.toDomain(): CloudEvent =
    ProtoDeserializer(this).read(::fromSpecVersion)

fun CloudEvent.toTransfer(): ProtoCloudEvent =
    ProtoSerializer.toProto(this)
