pluginManagement {
    repositories {
        gradlePluginPortal()
    }
    includeBuild("../build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        mavenLocal()
    }

    versionCatalogs {
        create("libs") {
            version("grpc", "1.56.0")
            version("cloudevents", "2.5.0")
            version("micronaut", "4.1.1")
            version("arrow", "1.2.1")

            library("kotlin-serialization-json", "org.jetbrains.kotlinx", "kotlinx-serialization-json").version("1.5.1")
            library("kotlinx-coroutines", "org.jetbrains.kotlinx", "kotlinx-coroutines-core").version("1.7.1")
            library("cloudevents-core", "io.cloudevents", "cloudevents-core").versionRef("cloudevents")
            library("grpc-netty", "io.grpc", "grpc-netty-shaded").versionRef("grpc")
            library("arrow-core", "io.arrow-kt", "arrow-core").versionRef("arrow")
        }
    }
}
includeBuild("../clients/jvm")

rootProject.name = "examples"
include("autonomo", "clojure-repl", "simulation-test")
