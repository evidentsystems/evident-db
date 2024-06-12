plugins {
    kotlin("jvm") version "1.9.21"
}

group = "com.evidentdb"
version = "0.1.0-alpha-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}