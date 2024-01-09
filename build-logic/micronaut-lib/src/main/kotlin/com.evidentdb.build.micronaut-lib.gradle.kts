plugins {
    id("com.evidentdb.build.kotlin")
    id("com.google.devtools.ksp")
    id("org.jetbrains.kotlin.plugin.allopen")
    id("io.micronaut.library")
}

dependencies {
    ksp("io.micronaut:micronaut-inject-kotlin")
    kspTest("io.micronaut:micronaut-inject-kotlin")
}
