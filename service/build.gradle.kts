import com.google.protobuf.gradle.*

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.protobuf)
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.arrow.core)

    implementation(project(":domain"))
    implementation(project(":adapters"))
    protobuf(project(":adapters"))

    implementation(libs.kafka.streams)

    implementation(libs.protobuf.java)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.kotlin.stub)
    implementation(libs.grpc.protobuf)

    testImplementation(libs.junit.api)
    testRuntimeOnly(libs.junit.engine)
}

protobuf {
    plugins {
        id("grpc")   { artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}" }
        id("grpckt") { artifact = "io.grpc:protoc-gen-grpc-kotlin:${libs.versions.grpckotlin.get()}:jdk8@jar" }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach { task ->
            task.plugins {
                id("grpc")
                id("grpckt")
            }
        }
    }
}

sourceSets {
    main {
        java {
            srcDirs(
                "build/generated/source/proto/main/grpc",
                "build/generated/source/proto/main/java",
                "build/generated/source/proto/main/grpckt",
            )
        }
    }
}

tasks.test {
    useJUnitPlatform()
}