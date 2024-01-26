plugins {
    id("com.evidentdb.build.kotlin")
    id("com.evidentdb.build.protobuf")
}

group = "com.evidentdb.server"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(project(":domain"))

    implementation(libs.arrow.core)
    implementation(libs.protobuf.java)
    implementation(libs.cloudevents.protobuf)
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
