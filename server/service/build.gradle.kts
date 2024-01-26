import com.google.protobuf.gradle.*

plugins {
    id("com.evidentdb.build.kotlin")
    id("com.evidentdb.build.protobuf")
}

group = "com.evidentdb.server"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.arrow.core)
    compileOnly(libs.slf4j.api)

    implementation(project(":domain"))
    implementation(project(":transfer"))

    implementation(libs.cloudevents.protobuf)
    implementation(libs.protobuf.java)
    compileOnly(libs.grpc.stub)
    implementation(libs.grpc.kotlin.stub)
    implementation(libs.grpc.protobuf)

    implementation(libs.micrometer.core)
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
                "build/generated/source/proto/main/java",
                "build/generated/source/proto/main/grpc",
                "build/generated/source/proto/main/grpckt",
            )
        }
    }
}
