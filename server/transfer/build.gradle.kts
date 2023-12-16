plugins {
    id("com.evidentdb.build.kotlin")
    id("com.evidentdb.build.protobuf")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))

    implementation(project(":domain"))

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