package com.evidentdb.kafka

const val DATABASE_LOG_TOPIC_PARTITIONS = 1
const val RETENTION_MS = -1

fun minInsyncReplicas(replication: Int) =
    replication.div(2) + 1
