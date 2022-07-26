plugins {
    id("org.jetbrains.kotlin.jvm")
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

    implementation(libs.protobuf.java)

    testImplementation(libs.junit.api)
    testRuntimeOnly(libs.junit.engine)
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("${project.properties["java.version"]}"))
    }
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
