pluginManagement {
    repositories {
        gradlePluginPortal()
    }
    includeBuild("../build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
includeBuild("../clients")

rootProject.name = "evident-db-examples" // the component name
include("autonomo")
