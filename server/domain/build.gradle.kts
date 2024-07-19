plugins {
    id("com.evidentdb.build.kotlin")
}

group = "com.evidentdb.server"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(libs.arrow.core)
    implementation(libs.arrow.resilience)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.cloudevents.core)
    implementation(libs.commons.codec)
    compileOnly(libs.slf4j.api)

    testImplementation(libs.kotlinx.coroutines.test)
}
