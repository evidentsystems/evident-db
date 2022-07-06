plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.protobuf)
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))

    implementation(project(":domain"))

    compileOnly(libs.kafka.streams)
    implementation(libs.cloudevents.kafka)
    implementation(libs.cloudevents.protobuf)
    implementation(libs.protobuf.java)

    testImplementation(libs.junit.api)
    testRuntimeOnly(libs.junit.engine)
}

tasks.test {
    useJUnitPlatform()
}
