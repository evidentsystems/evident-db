package com.evidentdb.kafka

import io.cloudevents.CloudEvent
import org.apache.kafka.common.utils.Utils
import java.util.*

fun partitionBySource(event: CloudEvent, numPartitions: Int): Int {
    val sourceBytes = event.source
        ?.toString()
        ?.toByteArray(Charsets.UTF_8)
        ?: return 0
    return Utils.toPositive(Utils.murmur2(sourceBytes)) % numPartitions
}

fun partitionByDatabaseId(databaseId: UUID, numPartitions: Int): Int {
    val sourceBytes = databaseId
        .toString()
        .toByteArray(Charsets.UTF_8)
    return Utils.toPositive(Utils.murmur2(sourceBytes)) % numPartitions
}