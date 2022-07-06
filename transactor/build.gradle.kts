plugins {
    alias(libs.plugins.kotlin.jvm)
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.arrow.core)

    implementation(project(":domain"))
    implementation(project(":adapters"))

    implementation(libs.kafka.streams)
    implementation(libs.cloudevents.api)

    testImplementation(libs.kafka.streams.test.utils)
    testImplementation(libs.junit.api)
    testRuntimeOnly(libs.junit.engine)
}

tasks.test {
    useJUnitPlatform()
}
