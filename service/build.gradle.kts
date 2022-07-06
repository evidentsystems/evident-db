plugins {
    alias(libs.plugins.kotlin.jvm)
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.arrow.core)

    implementation(project(":domain"))
    implementation(project(":adapters"))

    implementation(libs.kafka.streams)
}

tasks.test {
    useJUnitPlatform()
}
