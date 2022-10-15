plugins {
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.kapt")
    id("com.github.johnrengelman.shadow") version "7.1.1"
    id("io.micronaut.application") version "3.5.0"
}

version = "0.1.0-SNAPSHOT"
group = "com.evidentdb"

dependencies {
    implementation(libs.arrow.core)
    implementation(libs.kotlinx.coroutines)

    implementation(project(":adapters"))
    implementation(project(":domain"))
    implementation(project(":service"))
    implementation(project(":transactor"))

    implementation(libs.kafka.streams)

    implementation(libs.grpc.kotlin.stub)
    implementation("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.grpc:micronaut-grpc-server-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("jakarta.annotation:jakarta.annotation-api")
    runtimeOnly("ch.qos.logback:logback-classic")
    implementation("io.micronaut:micronaut-validation")
    implementation("io.micronaut.micrometer:micronaut-micrometer-registry-prometheus")
    implementation("io.micronaut:micronaut-management")
    implementation("io.micronaut:micronaut-http-server-netty")
    implementation("io.micronaut.kafka:micronaut-kafka")

    testImplementation("io.micronaut:micronaut-http-client")
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("${project.properties["java.version"]}"))
    }
}

micronaut {
    version.set(libs.versions.micronaut)
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("com.evidentdb.*")
    }
}

application {
    mainClass.set("com.evidentdb.app.ApplicationKt")
    applicationDefaultJvmArgs = listOf("-Xmx4g", "-Xms4g")
}