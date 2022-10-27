package com.evidentdb.app

import io.micronaut.context.env.Environment
import io.micronaut.runtime.Micronaut
import picocli.CommandLine
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Spec

fun main(args: Array<String>) {
    CommandLine(Cli::class.java).execute(*args)
}

@CommandLine.Command(
    name = "evidentdb",
    description = ["The Event Sourcing, Event Streaming database backed by Apache Kafka."],
    mixinStandardHelpOptions = true,
    version = ["0.1.0"]
)
class Cli : Runnable {
    @CommandLine.Option(
        names = ["-k", "--kafka-bootstrap"],
        description = ["The kafka bootstrap.servers config as a comma-separated list " +
                "of `host:port,host:port`, " +
                "per https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers"],
        defaultValue = "localhost:9092",
        required = true
    )
    var kafkaBootstrapServers: String = "localhost:9092"

    @CommandLine.Option(
        names = ["-t", "--evidentdb-tenant"],
        description = ["The unique namespace for topics, etc. to facilitate " +
                "multiple EvidentDB clusters running against " +
                "a single Kafka cluster. All nodes running with the same tenant " +
                "and Kafka cluster are members of the same EvidentDB cluster."],
        defaultValue = "default-tenant",
        required = true
    )
    var tenant: String = "default-tenant"

    @Spec
    lateinit var spec: CommandSpec

    override fun run() {
        throw CommandLine.ParameterException(
            spec.commandLine(),
            "Missing required subcommand"
        )
    }

    @CommandLine.Command(
        description = ["Bootstrap a new tenant within a Kafka cluster by creating " +
                "the internal topics used by EvidentDB."],
        mixinStandardHelpOptions = true,
    )
    fun bootstrap(
        @CommandLine.Option(
            names = ["-p", "--partitions"],
            description = ["The number of partitions to create when creating EvidentDB's internal topics."],
            defaultValue = "6",
            required = true
        )
        partitions: Int = 6,
        @CommandLine.Option(
            names = ["-r", "--replication"],
            description = ["The replication factor to establish when creating EvidentDB's internal topics."],
            defaultValue = "3",
            required = true
        )
        replication: Int = 3,
        @CommandLine.Option(
            names = ["-c", "--compression-type"],
            description = ["The compression.type to configure when creating EvidentDB's internal topics."],
            defaultValue = "uncompressed",
            required = true
        )
        compressionType: String = "uncompressed"
    ) {
        Micronaut
            .build()
            .properties(mapOf(
                "evidentdb.tenant" to tenant,
                "evidentdb.topics.internal-commands.partitions" to partitions,
                "evidentdb.topics.internal-commands.replication" to replication,
                "evidentdb.topics.internal-commands.compression-type" to compressionType,
                "evidentdb.topics.internal-events.partitions" to partitions,
                "evidentdb.topics.internal-events.replication" to replication,
                "evidentdb.topics.internal-events.compression-type" to compressionType,
                "kafka.bootstrap.servers" to kafkaBootstrapServers,
                "grpc.server.enabled" to false,
                "endpoints.all.enabled" to false,
            ))
            .environments(Environment.CLI, "bootstrap")
            .start()
    }

    @CommandLine.Command(
        description = ["Run a node within a Kafka cluster and tenant."],
        mixinStandardHelpOptions = true,
    )
    fun node() {
        Micronaut
            .build()
            .eagerInitSingletons(true)
            .properties(mapOf(
                "evidentdb.tenant" to tenant,
                "kafka.bootstrap.servers" to kafkaBootstrapServers
            ))
            .environments("node")
            .start()
    }
}