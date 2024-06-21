plugins {
    id("com.evidentdb.build.micronaut-app")
    id("org.jetbrains.kotlin.plugin.serialization") version "1.8.22"
}

version = "0.1"
group = "com.evidentdb.test"

val kotlinVersion=project.properties.get("kotlinVersion")

dependencies {
    // EvidentDB client
    implementation("com.evidentdb:client-kotlin")
    implementation(libs.cloudevents.core)
    implementation(libs.grpc.netty)
    implementation(libs.arrow.core)

    // Kafka Streams
    implementation("org.apache.kafka:kafka-streams:3.7.0")

    // Micronaut
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut.serde:micronaut-serde-jackson")
    annotationProcessor("io.micronaut.serde:micronaut-serde-processor")
    annotationProcessor("io.micronaut:micronaut-http-validation")
    implementation("jakarta.annotation:jakarta.annotation-api")
    implementation("io.micronaut:micronaut-http-server-netty")
    runtimeOnly("com.fasterxml.jackson.module:jackson-module-kotlin")
    runtimeOnly("org.yaml:snakeyaml")
    testImplementation("io.micronaut:micronaut-http-client")

    // Logging
    runtimeOnly("ch.qos.logback:logback-classic")
}


application {
    mainClass = "com.evidentdb.test.simulation.ApplicationKt"
}

micronaut {
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("com.evidentdb.test.simulation.*")
    }
    aot {
    // Please review carefully the optimizations enabled below
    // Check https://micronaut-projects.github.io/micronaut-aot/latest/guide/ for more details
        optimizeServiceLoading = false
        convertYamlToJava = false
        precomputeOperations = true
        cacheEnvironment = true
        optimizeClassLoading = true
        deduceEnvironment = true
        optimizeNetty = true
        replaceLogbackXml = true
    }
}
