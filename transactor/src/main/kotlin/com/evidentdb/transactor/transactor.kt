package com.evidentdb.transactor

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain.*
import com.evidentdb.kafka.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Exception

class KafkaStreamsCommandHandler(
    private val adminClient: AdminClient,
    private val databaseTopicReplication: Short,
    private val databaseTopicCompressionType: String,
): CommandHandler {
    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(KafkaStreamsCommandHandler::class.java)
    }
    override lateinit var databaseReadModel: DatabaseReadModel
    override lateinit var batchSummaryReadModel: BatchSummaryReadModel

    fun init(
        databaseStore: DatabaseReadOnlyKeyValueStore,
        logStore: DatabaseLogReadOnlyKeyValueStore,
        batchStore: BatchIndexReadOnlyKeyValueStore,
    ) {
        this.databaseReadModel = DatabaseReadModelStore(
            databaseStore,
            logStore,
        )
        this.batchSummaryReadModel = BatchSummaryReadOnlyStore(batchStore, logStore)
    }

    override suspend fun createDatabaseTopic(
        database: DatabaseName,
        topicName: TopicName
    ): Either<DatabaseTopicCreationError, TopicName> {
        return try {
            adminClient.createTopics(
                listOf(
                    NewTopic(
                        topicName,
                        DATABASE_LOG_TOPIC_PARTITIONS,
                        databaseTopicReplication,
                    ).configs(
                        mapOf(
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG to minInsyncReplicas(databaseTopicReplication.toInt()).toString(),
                            TopicConfig.RETENTION_MS_CONFIG to RETENTION_MS.toString(),
                            TopicConfig.COMPRESSION_TYPE_CONFIG to databaseTopicCompressionType
                        )
                    )
                )
            ).all().get()
            topicName.right()
        } catch (e: Exception) {
            LOGGER.error(
                "Failed to create database $database event log topic $topicName",
                e
            )
            DatabaseTopicCreationError(
                database.value,
                topicName
            ).left()
        }
    }

    override suspend fun deleteDatabaseTopic(
        database: DatabaseName,
        topicName: TopicName
    ): Either<DatabaseTopicDeletionError, Unit> {
        return try {
            adminClient.deleteTopics(listOf(topicName)).all().get()
            Unit.right()
        } catch (e: Exception) {
            LOGGER.error(
                "Failed to delete database $database event log topic $topicName",
                e
            )
            DatabaseTopicDeletionError(
                database.value,
                topicName
            ).left()
        }
    }
}
