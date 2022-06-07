plugins {
    kotlin("jvm") version "1.6.21"
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("io.arrow-kt:arrow-core:1.0.1")

    implementation(project(":domain"))
    implementation(project(":util"))

    implementation("org.apache.kafka:kafka-streams:3.1.0")
    implementation("io.cloudevents:cloudevents-api:2.3.0")
}