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
    implementation(libs.kotlinx.coroutines.test)
    implementation(libs.cloudevents.core)
    implementation(libs.commons.codec)
    implementation("org.junit.jupiter:junit-jupiter-api:5.9.3")

    testImplementation(project(":test"))
}
