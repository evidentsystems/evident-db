package com.evidentdb.kafka

import io.cloudevents.CloudEvent
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class EventSourcePartitioner: Partitioner {
    override fun configure(configs: MutableMap<String, *>?) {}
    override fun close() {}

    override fun partition(
        topic: String?,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster?
    ): Int =
        if (value != null && value is CloudEvent) {
            partitionBySource(value, cluster?.partitionCountForTopic(topic)!!)
        } else {
            0
        }
}