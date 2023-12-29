plugins {
    id("com.evidentdb.build.kotlin")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(project(":domain"))
    implementation(libs.arrow.core)
    implementation(libs.cloudevents.core)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.coroutines.test)
    implementation("org.junit.jupiter:junit-jupiter-api:5.9.3")
}
