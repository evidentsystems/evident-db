package com.evidentdb.kafka

import io.cloudevents.CloudEvent
import org.apache.kafka.streams.processor.RecordContext
import org.apache.kafka.streams.processor.TopicNameExtractor

const val TOPIC_HEADER = "topic"
const val DATABASE_LOG_TOPIC_PARTITIONS = 1
const val RETENTION_MS = -1

fun minInsyncReplicas(replication: Int) =
    replication.div(2) + 1

class DatabaseTopicNameExtractor: TopicNameExtractor<String, CloudEvent> {
    override fun extract(
        key: String,
        value: CloudEvent,
        recordContext: RecordContext
    ): String {
        val topic = recordContext
            .headers()
            .lastHeader(TOPIC_HEADER)
            .value()
            .toString(Charsets.UTF_8)
        recordContext.headers().remove(TOPIC_HEADER)
        return topic
    }
}