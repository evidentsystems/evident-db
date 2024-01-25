plugins {
    id("com.evidentdb.build.kotlin")
    id("org.jetbrains.kotlin.plugin.allopen")
    id("com.google.devtools.ksp")
    id("io.micronaut.library")
}

dependencies {
    ksp("io.micronaut:micronaut-inject-kotlin")
    ksp("io.micronaut.serde:micronaut-serde-processor")

    kspTest("io.micronaut:micronaut-inject-kotlin")
    kspTest("io.micronaut.serde:micronaut-serde-processor")
}
