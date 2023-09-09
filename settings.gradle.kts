rootProject.name = "evident-db"
include("test", "domain", "adapters", "service", "transactor", "app", "client")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }

    versionCatalogs {
        create("libs") {
            version("kotlin", "1.9.0")
            version("kafka", "3.5.1")
            version("caffeine", "3.1.1")
            version("cloudevents", "2.5.0")
            version("protobuf-java", "3.23.3")
            version("grpc", "1.56.0")
            version("grpckotlin", "1.3.0")
            version("micronaut", "3.9.3")
            version("micrometer", "1.11.1")
            version("slf4j", "1.7.36")
            version("junit", "5.9.3")

            plugin("protobuf", "com.google.protobuf").version("0.9.3")
            plugin("kotlin-jvm", "org.jetbrains.kotlin.jvm").versionRef("kotlin")
            library("kotlinx-coroutines", "org.jetbrains.kotlinx", "kotlinx-coroutines-core").version("1.7.1")
            library("kotlinx-coroutines-jdk8", "org.jetbrains.kotlinx", "kotlinx-coroutines-jdk8").version("1.7.1")
            library("arrow-core", "io.arrow-kt", "arrow-core").version("1.1.5")
            library("valiktor-core", "org.valiktor", "valiktor-core").version("0.12.0")
            library("kafka-streams", "org.apache.kafka", "kafka-streams").versionRef("kafka")
            library("kafka-streams-test-utils", "org.apache.kafka", "kafka-streams-test-utils").versionRef("kafka")
            library("cloudevents-api", "io.cloudevents", "cloudevents-api").versionRef("cloudevents")
            library("cloudevents-core", "io.cloudevents", "cloudevents-core").versionRef("cloudevents")
            library("cloudevents-kafka", "io.cloudevents", "cloudevents-kafka").versionRef("cloudevents")
            library("cloudevents-protobuf", "io.cloudevents", "cloudevents-protobuf").versionRef("cloudevents")
            library("caffeine", "com.github.ben-manes.caffeine", "caffeine").versionRef("caffeine")
            library("protobuf-java", "com.google.protobuf", "protobuf-java").versionRef("protobuf-java")
            library("grpc-stub", "io.grpc", "grpc-stub").versionRef("grpc")
            library("grpc-protobuf", "io.grpc", "grpc-protobuf").versionRef("grpc")
            library("grpc-netty", "io.grpc", "grpc-netty-shaded").versionRef("grpc")
            library("grpc-kotlin-stub", "io.grpc", "grpc-kotlin-stub").versionRef("grpckotlin")
            library("micrometer-core", "io.micrometer", "micrometer-core").versionRef("micrometer")
            library("commons-codec", "commons-codec", "commons-codec").version("1.15")
            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef("slf4j")
            library("junit-api", "org.junit.jupiter", "junit-jupiter-api").versionRef("junit")
            library("junit-engine", "org.junit.jupiter", "junit-jupiter-engine").versionRef("junit")
        }
    }
}
