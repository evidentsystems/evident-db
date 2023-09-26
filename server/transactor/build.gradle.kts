plugins {
    id("com.evidentdb.build.kotlin")
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
    implementation(libs.micrometer.core)

    testImplementation(project(":test"))
    testImplementation(libs.kafka.streams.test.utils)
}
