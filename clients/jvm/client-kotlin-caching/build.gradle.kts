plugins {
    id("com.evidentdb.build.kotlin")
    `java-library`
    `maven-publish`
}

group = "com.evidentdb"
version = "0.1.0-alpha-SNAPSHOT"

dependencies {
    api(project(":client-core"))

    // Logging
    compileOnly(libs.slf4j.api)

    // Cache
    api(libs.caffeine)

    // Test dependencies
    testImplementation("io.grpc:grpc-netty:1.65.0")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}
