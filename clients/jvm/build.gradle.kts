import com.google.protobuf.gradle.*

plugins {
    id("com.evidentdb.build.kotlin")
    id("com.evidentdb.build.protobuf")
    `java-library`
    `maven-publish`
}

group = "com.evidentdb"
version = "0.1.0-alpha-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.coroutines.jdk8)
    implementation(libs.arrow.core)
    implementation(libs.commons.codec)
    compileOnly(libs.slf4j.api)

    implementation(libs.cloudevents.core)
    implementation(libs.cloudevents.protobuf)

    implementation(libs.protobuf.java)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.kotlin.stub)

    // Cache
    implementation(libs.caffeine)
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

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}
