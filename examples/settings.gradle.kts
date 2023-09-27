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

            library("kotlinx-coroutines", "org.jetbrains.kotlinx", "kotlinx-coroutines-core").version("1.7.1")
            library("cloudevents-core", "io.cloudevents", "cloudevents-core").versionRef("cloudevents")
            library("grpc-netty", "io.grpc", "grpc-netty-shaded").versionRef("grpc")
        }
    }
}
includeBuild("../clients")

rootProject.name = "examples"
include("autonomo") //, "clojure-repl")
