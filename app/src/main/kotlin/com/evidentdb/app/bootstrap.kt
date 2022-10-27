package com.evidentdb.app

import io.micronaut.context.annotation.*
import jakarta.annotation.PostConstruct
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutionException
import kotlin.system.exitProcess


@Context
@Requires(env = ["bootstrap"])
class BootstrapRunner(
    private val adminClient: AdminClient,
    private val evidentDbConfig: EvidentDbConfig,
) {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(BootstrapRunner::class.java)
    }

    @PostConstruct
    fun start() {
        val result = adminClient.createTopics(
            listOf(
                evidentDbConfig.topics.internalCommands.let { commandTopic ->
                    NewTopic(
                        commandTopic.name,
                        commandTopic.partitions,
                        commandTopic.replication.toShort()
                    ).configs(
                        mapOf(
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG to minInsyncReplicas(commandTopic.replication).toString(),
                            TopicConfig.RETENTION_MS_CONFIG to RETENTION_MS.toString(),
                            TopicConfig.COMPRESSION_TYPE_CONFIG to commandTopic.compressionType
                        )
                    )
                },
                evidentDbConfig.topics.internalEvents.let { eventTopic ->
                    NewTopic(
                        eventTopic.name,
                        eventTopic.partitions,
                        eventTopic.replication.toShort()
                    ).configs(
                        mapOf(
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG to minInsyncReplicas(eventTopic.replication).toString(),
                            TopicConfig.RETENTION_MS_CONFIG to RETENTION_MS.toString(),
                            TopicConfig.COMPRESSION_TYPE_CONFIG to eventTopic.compressionType
                        )
                    )
                }
            ),
            CreateTopicsOptions()
        )
        for ((topicName, resultFuture) in result.values()) {
            try {
                LOGGER.info("Creating topic: $topicName")
                resultFuture.get()
            } catch (e: ExecutionException) {
                if (e.cause is TopicExistsException)
                    LOGGER.info("Topic \"$topicName\" already exists")
                else {
                    LOGGER.error("Error creating topic \"$topicName\"", e)
                    exitProcess(100)
                }
            } catch (e: Exception) {
                LOGGER.error("Error creating topic \"$topicName\"", e)
                exitProcess(100)
            }
        }
        exitProcess(0)
    }
}
