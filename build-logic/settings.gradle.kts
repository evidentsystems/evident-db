pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

rootProject.name = "evident-db-build-logic"
include("kotlin", "kapt", "protobuf", "micronaut-app")
