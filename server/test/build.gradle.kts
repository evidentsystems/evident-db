plugins {
    id("com.evidentdb.build.kotlin")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.cloudevents.core)
}
