import com.google.protobuf.gradle.*

plugins {
    alias(libs.plugins.kotlin.jvm)
    id("org.jetbrains.kotlin.kapt") version "1.6.21"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.6.21"
    id("com.github.johnrengelman.shadow") version "7.1.1"
    id("io.micronaut.application") version "3.5.0"
    id("com.google.protobuf") version "0.8.18"
}

micronaut {
    version.set(libs.versions.micronaut)
}

version = "0.1"
group = "com.evidentdb"

dependencies {
    implementation(project(":domain"))
    implementation(project(":adapters"))
    implementation(project(":service"))
    implementation(project(":transactor"))

    implementation("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.grpc:micronaut-grpc-server-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("jakarta.annotation:jakarta.annotation-api")
    runtimeOnly("ch.qos.logback:logback-classic")
    implementation("io.micronaut:micronaut-validation")

    testImplementation("io.micronaut:micronaut-http-client")
}


application {
    mainClass.set("com.evidentdb.ApplicationKt")
}
java {
    sourceCompatibility = JavaVersion.toVersion("11")
}

tasks {
    compileKotlin {
        kotlinOptions {
            jvmTarget = "11"
        }
    }
    compileTestKotlin {
        kotlinOptions {
            jvmTarget = "11"
        }
    }
}
sourceSets {
    main {
        java {
            srcDirs("build/generated/source/proto/main/grpc")
            srcDirs("build/generated/source/proto/main/java")
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.17.2"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.39.0"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
            }
        }
    }
}

micronaut {
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("com.evidentdb.*")
    }
}