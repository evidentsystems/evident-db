plugins {
    id("com.evidentdb.build.kotlin")
    `java-library`
    `maven-publish`
}

group = "com.evidentdb"
version = "0.1.0-alpha-SNAPSHOT"

dependencies {
    implementation(project(":client-api"))
    implementation(project(":client-kotlin-caching"))

    // Platform
    compileOnly(libs.slf4j.api)

    // Test dependencies
    testImplementation("io.grpc:grpc-netty:1.59.0")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}
