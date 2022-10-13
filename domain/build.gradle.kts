plugins {
    id("org.jetbrains.kotlin.jvm")
}

group = "com.evidentdb"
version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.arrow.core)
    implementation(libs.cloudevents.core)
    implementation(libs.valiktor.core)
    implementation(libs.commons.codec)

    testImplementation(project(":test"))
    testImplementation(libs.kotlinx.coroutines)
    testImplementation(libs.junit.api)
    testRuntimeOnly(libs.junit.engine)
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("${project.properties["java.version"]}"))
    }
}

tasks.test {
    useJUnitPlatform()
}