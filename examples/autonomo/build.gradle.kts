plugins {
    id("com.evidentdb.build.micronaut-app")
    id("org.jetbrains.kotlin.plugin.serialization") version "1.8.22"
    // id("gg.jte.gradle") version "1.12.1"
}

version = "0.1.0-alpha-SNAPSHOT"
group = "com.evidentdb.examples.autonomo"

dependencies {
    implementation(libs.kotlinx.coroutines)

    // EvidentDB Client
    implementation("com.evidentdb:client")
    implementation(libs.cloudevents.core)
    implementation(libs.grpc.netty)

    // Micronaut App Framework
    compileOnly("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut:micronaut-jackson-databind")
    implementation("io.micronaut.serde:micronaut-serde-jackson")
    annotationProcessor("io.micronaut.serde:micronaut-serde-processor")
//    implementation("io.micronaut.views:micronaut-views-jte")
    annotationProcessor("io.micronaut:micronaut-http-validation")
    implementation("jakarta.annotation:jakarta.annotation-api")
    implementation("io.micronaut:micronaut-http-server-netty")
    runtimeOnly("org.yaml:snakeyaml")
    testImplementation("io.micronaut:micronaut-http-client")

    // Logging
    runtimeOnly("ch.qos.logback:logback-classic")
}

application {
    mainClass.set("com.evidentdb.examples.autonomo.ApplicationKt")
}

micronaut {
    version.set(libs.versions.micronaut)
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("com.evidentdb.examples.autonomo.*")
    }
}

graalvmNative {
    toolchainDetection.set(false)
    metadataRepository {
        enabled.set(false)
    }
}

//jte {
//    sourceDirectory.set(file("src/main/jte").toPath())
//    generate()
//}
