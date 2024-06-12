plugins {
    id("com.evidentdb.build.kotlin")
    `java-library`
    `maven-publish`
}

group = "com.evidentdb"
version = "0.1.0-alpha-SNAPSHOT"

dependencies {
    implementation(project(":client-api"))
    api(project(":client-kotlin"))

    // Platform
    compileOnly(libs.slf4j.api)

    // gRPC
    api(libs.protobuf.java)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.grpc.kotlin.stub)

    // Cache
    api(libs.caffeine)

    // Test dependencies
    testImplementation("io.grpc:grpc-netty:1.64.0")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}
