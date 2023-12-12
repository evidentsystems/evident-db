plugins {
    id("com.evidentdb.build.kotlin")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.arrow.core)
    implementation(libs.arrow.resilience)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.cloudevents.core)
    implementation(libs.commons.codec)

    testImplementation(project(":test"))
}
