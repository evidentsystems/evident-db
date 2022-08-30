rootProject.name = "evident-db"
include("test", "domain", "adapters", "service", "transactor", "app")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }

    versionCatalogs {
        create("libs") {
            version("kotlin", "1.7.10")
            version("kafka", "3.2.0")
            version("cloudevents", "2.3.0")
            version("protobuf-java", "3.21.2")
            version("grpc", "1.47.0")
            version("grpckotlin", "1.3.0")
            version("junit", "5.6.0")
            version("micronaut", "3.5.3")
            version("slf4j", "1.7.36")
            version("valiktor", "0.12.0")

            plugin("protobuf", "com.google.protobuf").version("0.8.18")
            plugin("kotlin-jvm", "org.jetbrains.kotlin.jvm").versionRef("kotlin")
            library("kotlinx-coroutines", "org.jetbrains.kotlinx", "kotlinx-coroutines-core").version("1.6.2")
            library("arrow-core", "io.arrow-kt", "arrow-core").version("1.0.1")
            library("valiktor-core", "org.valiktor", "valiktor-core").versionRef("valiktor")
            library("kafka-streams", "org.apache.kafka", "kafka-streams").versionRef("kafka")
            library("kafka-streams-test-utils", "org.apache.kafka", "kafka-streams-test-utils").versionRef("kafka")
            library("cloudevents-api", "io.cloudevents", "cloudevents-api").versionRef("cloudevents")
            library("cloudevents-core", "io.cloudevents", "cloudevents-core").versionRef("cloudevents")
            library("cloudevents-kafka", "io.cloudevents", "cloudevents-kafka").versionRef("cloudevents")
            library("cloudevents-protobuf", "io.cloudevents", "cloudevents-protobuf").versionRef("cloudevents")
            library("protobuf-java", "com.google.protobuf", "protobuf-java").versionRef("protobuf-java")
            library("grpc-stub", "io.grpc", "grpc-stub").versionRef("grpc")
            library("grpc-kotlin-stub", "io.grpc", "grpc-kotlin-stub").versionRef("grpckotlin")
            library("grpc-protobuf", "io.grpc", "grpc-protobuf").versionRef("grpc")
            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef("slf4j")
            library("junit-api", "org.junit.jupiter", "junit-jupiter-api").versionRef("junit")
            library("junit-engine", "org.junit.jupiter", "junit-jupiter-engine").versionRef("junit")
        }
    }
}