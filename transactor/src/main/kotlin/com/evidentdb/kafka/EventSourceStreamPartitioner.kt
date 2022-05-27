package com.evidentdb.kafka

import io.cloudevents.CloudEvent
import org.apache.kafka.streams.processor.StreamPartitioner
import java.util.*

class EventSourceStreamPartitioner: StreamPartitioner<UUID, CloudEvent> {
    override fun partition(topic: String?, key: UUID?, value: CloudEvent?, numPartitions: Int): Int =
        partitionBySource(value!!, numPartitions)
}