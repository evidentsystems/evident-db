package com.evidentdb.kafka

import com.evidentdb.domain_model.DatabaseName
import io.cloudevents.CloudEvent
import org.apache.kafka.common.utils.Utils

fun partitionBySource(event: CloudEvent, numPartitions: Int): Int {
    val sourceBytes = event.source
        ?.toString()
        ?.toByteArray(Charsets.UTF_8)
        ?: return 0
    return Utils.toPositive(Utils.murmur2(sourceBytes)) % numPartitions
}

fun partitionByDatabase(databaseName: DatabaseName, numPartitions: Int): Int {
    val sourceBytes = databaseName.value.toByteArray(Charsets.UTF_8)
    return Utils.toPositive(Utils.murmur2(sourceBytes)) % numPartitions
}