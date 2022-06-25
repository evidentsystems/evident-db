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
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.2")
    implementation("io.arrow-kt:arrow-core:1.0.1")

    implementation(project(":domain"))
    implementation(project(":kafka"))

    implementation("org.apache.kafka:kafka-streams:3.2.0")
    implementation("io.cloudevents:cloudevents-api:2.3.0")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.2.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.test {
    useJUnitPlatform()
}
