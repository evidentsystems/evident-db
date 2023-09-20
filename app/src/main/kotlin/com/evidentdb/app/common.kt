package com.evidentdb.app

import io.micronaut.context.annotation.ConfigurationInject
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.core.bind.annotation.Bindable
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull

@ConfigurationProperties("evidentdb")
data class EvidentDbConfig
@ConfigurationInject constructor(
    @NotBlank
    val tenant: String,
    @NotNull
    val topics: TopicsConfig
) {
    @ConfigurationProperties("topics")
    data class TopicsConfig
    @ConfigurationInject constructor(
        @NotNull
        val internalCommands: InternalCommandsConfig,
        @NotNull
        val internalEvents: InternalEventsConfig,
        @NotNull
        val databaseTopics: DatabaseTopicsConfig
    ) {
        @ConfigurationProperties("internal-commands")
        data class InternalCommandsConfig
        @ConfigurationInject constructor(
            @NotBlank
            val name: String,

            @Bindable(defaultValue = "4")
            @Min(1)
            @Max(24)
            val partitions: Int,

            @Bindable(defaultValue = "3")
            @Min(1)
            val replication: Int,

            @Bindable(defaultValue = "uncompressed")
            @NotBlank
            val compressionType: String
        )

        @ConfigurationProperties("internal-events")
        data class InternalEventsConfig
        @ConfigurationInject constructor(
            @NotBlank
            val name: String,

            @Bindable(defaultValue = "4")
            @Min(1)
            @Max(24)
            val partitions: Int,

            @Bindable(defaultValue = "3")
            @Min(1)
            val replication: Int,

            @Bindable(defaultValue = "uncompressed")
            @NotBlank
            val compressionType: String
        )

        @ConfigurationProperties("database-topics")
        data class DatabaseTopicsConfig
        @ConfigurationInject constructor(
            @Bindable(defaultValue = "3")
            @Min(1)
            val replication: Int,

            @Bindable(defaultValue = "uncompressed")
            @NotBlank
            val compressionType: String
        )
    }
}
