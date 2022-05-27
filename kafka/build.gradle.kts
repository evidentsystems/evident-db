plugins {
    kotlin("jvm") version "1.5.31"
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))

    project(":cloudevents")
    compileOnly("org.apache.kafka:kafka-streams:3.1.0")
}