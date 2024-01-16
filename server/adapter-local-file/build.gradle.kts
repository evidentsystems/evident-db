plugins {
    id("com.evidentdb.build.micronaut-lib")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

micronaut {
    version.set(libs.versions.micronaut)
}

dependencies {
    implementation(libs.arrow.core)
    implementation(libs.kotlinx.coroutines)

    implementation(project(":domain"))

    implementation(libs.cloudevents.core)
    implementation("jakarta.annotation:jakarta.annotation-api")

    implementation("org.rocksdb:rocksdbjni:8.9.1")

    testImplementation(project(":test"))
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}