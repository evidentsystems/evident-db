package com.evidentdb.app

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.core.bind.annotation.Bindable
import javax.validation.constraints.Max
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotNull

@ConfigurationProperties("evidentdb")
interface EvidentDbConfig {
    @get:NotBlank
    val tenant: String

    @get:NotNull
    val topics: TopicsConfig

    @ConfigurationProperties("topics")
    interface TopicsConfig {
        @get:NotNull
        val internalCommands: InternalCommandsConfig

        @get:NotNull
        val internalEvents: InternalEventsConfig

        @get:NotNull
        val databaseTopics: DatabaseTopicsConfig

        @ConfigurationProperties("internal-commands")
        interface InternalCommandsConfig {
            @get:NotBlank
            val name: String

            @get:Bindable(defaultValue = "4")
            @get:Min(1)
            @get:Max(24)
            val partitions: Int

            @get:Bindable(defaultValue = "3")
            @get:Min(1)
            val replication: Int

            @get:Bindable(defaultValue = "uncompressed")
            @get:NotBlank
            val compressionType: String
        }

        @ConfigurationProperties("internal-events")
        interface InternalEventsConfig {
            @get:NotBlank
            val name: String

            @get:Bindable(defaultValue = "4")
            @get:Min(1)
            @get:Max(24)
            val partitions: Int

            @get:Bindable(defaultValue = "3")
            @get:Min(1)
            val replication: Int

            @get:Bindable(defaultValue = "uncompressed")
            @get:NotBlank
            val compressionType: String
        }

        @ConfigurationProperties("database-topics")
        interface DatabaseTopicsConfig {
            @get:Bindable(defaultValue = "3")
            @get:Min(1)
            val replication: Int

            @get:Bindable(defaultValue = "uncompressed")
            @get:NotBlank
            val compressionType: String
        }
    }
}
