pluginManagement {
    repositories {
        gradlePluginPortal()
    }
    includeBuild("../../build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }

    versionCatalogs {
        create("libs") {
            version("arrow", "1.2.1")
            version("caffeine", "3.1.1")
            version("cloudevents", "2.5.0")
            version("protobuf-java", "3.25.1")
            version("grpc", "1.56.0")
            version("grpckotlin", "1.3.0")
            version("slf4j", "1.7.36")

            library("kotlinx-coroutines", "org.jetbrains.kotlinx", "kotlinx-coroutines-core").version("1.7.1")
            library("kotlinx-coroutines-jdk8", "org.jetbrains.kotlinx", "kotlinx-coroutines-jdk8").version("1.7.1")
            library("arrow-core", "io.arrow-kt", "arrow-core").versionRef("arrow")
            library("caffeine", "com.github.ben-manes.caffeine", "caffeine").versionRef("caffeine")
            library("cloudevents-core", "io.cloudevents", "cloudevents-core").versionRef("cloudevents")
            library("cloudevents-protobuf", "io.cloudevents", "cloudevents-protobuf").versionRef("cloudevents")
            library("protobuf-java", "com.google.protobuf", "protobuf-java").versionRef("protobuf-java")
            library("grpc-stub", "io.grpc", "grpc-stub").versionRef("grpc")
            library("grpc-protobuf", "io.grpc", "grpc-protobuf").versionRef("grpc")
            library("grpc-kotlin-stub", "io.grpc", "grpc-kotlin-stub").versionRef("grpckotlin")
            library("commons-codec", "commons-codec", "commons-codec").version("1.15")
            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef("slf4j")
        }
    }
}

rootProject.name = "clients.jvm"
include(":client-api")
include(":client-java")
include(":client-kotlin")
include(":client-java-caching")
include(":client-kotlin-caching")
