import com.google.protobuf.gradle.id

plugins {
    id("com.evidentdb.build.kotlin")
    id("com.evidentdb.build.protobuf")
    `java-library`
}

group = "com.evidentdb"
version = "0.1.0-alpha-SNAPSHOT"

dependencies {
    api(libs.kotlinx.coroutines)
    api(libs.arrow.core)
    api(libs.commons.codec)

    // CloudEvents
    api(libs.cloudevents.core)
    api(libs.cloudevents.protobuf)

    // gRPC
    api(libs.protobuf.java)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.grpc.kotlin.stub)
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
