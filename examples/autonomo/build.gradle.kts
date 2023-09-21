import com.google.protobuf.gradle.id

plugins {
    id("org.jetbrains.kotlin.jvm")
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("io.micronaut.application") version "4.1.1"
    // id("gg.jte.gradle") version "1.12.1"
    id("com.google.protobuf") version "0.9.3"
}

version = "0.1.0-alpha-SNAPSHOT"
group = "com.evidentdb.examples.autonomo"

dependencies {
    // EvidentDB Client
    implementation(project(":client"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.cloudevents.core)
    implementation(libs.grpc.netty)

    // Serialization
    implementation("com.google.protobuf:protobuf-kotlin:3.21.12")
    implementation("com.google.protobuf:protobuf-java-util:3.21.12")

    // Micronaut App Framework
    compileOnly("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut:micronaut-jackson-databind")
//    implementation("io.micronaut.views:micronaut-views-jte")
    implementation("jakarta.annotation:jakarta.annotation-api")
    runtimeOnly("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("io.micronaut:micronaut-http-server-netty")
    runtimeOnly("org.yaml:snakeyaml")

    // Logging
    runtimeOnly("ch.qos.logback:logback-classic")
}

kotlin {
    jvmToolchain {
        languageVersion.set(
            JavaLanguageVersion.of(
                "${project.properties["java.version"]}"
            )
        )
    }
}

application {
    mainClass.set("com.evidentdb.examples.autonomo.ApplicationKt")
}
graalvmNative.toolchainDetection = false
micronaut {
    version.set(libs.versions.micronaut)
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("com.evidentdb.examples.autonomo.*")
    }
}

//jte {
//    sourceDirectory.set(file("src/main/jte").toPath())
//    generate()
//}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.22.2"
    }

    generateProtoTasks {
        all().forEach {
            it.builtins {
                id("kotlin")
            }
        }
    }
}
