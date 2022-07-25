plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.protobuf)
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))

    implementation(project(":domain"))

    compileOnly(libs.kafka.streams)

    implementation(libs.cloudevents.kafka)
    implementation(libs.cloudevents.protobuf)
    protobuf(libs.cloudevents.protobuf)

    implementation(libs.protobuf.java)

    testImplementation(libs.junit.api)
    testRuntimeOnly(libs.junit.engine)
}

sourceSets {
    main {
        java {
            srcDirs(
                "build/generated/source/proto/main/java",
            )
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
