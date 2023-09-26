plugins {
    id("com.evidentdb.build.kotlin")
    id("com.evidentdb.build.protobuf")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.kotlinx.coroutines)

    implementation(project(":domain"))

    compileOnly(libs.kafka.streams)

    implementation(libs.cloudevents.kafka)
    implementation(libs.cloudevents.protobuf)

    implementation(libs.protobuf.java)
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
