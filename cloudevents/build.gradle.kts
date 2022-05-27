import java.util.regex.Pattern.compile

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

    compileOnly("org.apache.kafka:kafka-streams:3.1.0")
    implementation("io.cloudevents:cloudevents-protobuf:2.3.0")
    implementation("io.cloudevents:cloudevents-kafka:2.3.0")
}