import com.google.protobuf.gradle.*

plugins {
    id("org.jetbrains.kotlin.jvm")
    alias(libs.plugins.protobuf)
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    testImplementation(libs.junit.api)
    testRuntimeOnly(libs.junit.engine)
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("${project.properties["java.version"]}"))
    }
}

tasks.test {
    useJUnitPlatform()
}