plugins {
    id("org.jetbrains.kotlin.jvm")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.arrow.core)
    compileOnly(libs.slf4j.api)

    implementation(project(":domain"))
    implementation(project(":adapters"))

    implementation(libs.kafka.streams)
    implementation(libs.cloudevents.api)

    testImplementation(libs.kafka.streams.test.utils)
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
