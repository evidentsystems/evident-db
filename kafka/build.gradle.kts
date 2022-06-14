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

    implementation(project(":domain"))

    compileOnly("org.apache.kafka:kafka-streams:3.2.0")
    implementation("io.cloudevents:cloudevents-protobuf:2.3.0")
    implementation("io.cloudevents:cloudevents-kafka:2.3.0")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}