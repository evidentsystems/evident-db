import com.google.protobuf.gradle.*

plugins {
    id("com.evidentdb.build.micronaut-app")
    id("com.evidentdb.build.protobuf")
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

    // Serialization
    implementation("com.google.protobuf:protobuf-kotlin:3.21.12")
    implementation("com.google.protobuf:protobuf-java-util:3.21.12")

    // Micronaut App Framework
    compileOnly("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut:micronaut-jackson-databind")
//    implementation("io.micronaut.views:micronaut-views-jte")
    implementation("jakarta.annotation:jakarta.annotation-api")
    implementation("io.micronaut:micronaut-http-server-netty")
    runtimeOnly("org.yaml:snakeyaml")

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
    toolchainDetection = false
    metadataRepository {
        enabled = false
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
