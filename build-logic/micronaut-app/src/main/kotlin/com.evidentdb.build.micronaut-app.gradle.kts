plugins {
    id("com.evidentdb.build.kotlin")
    id("com.github.johnrengelman.shadow")
    id("io.micronaut.application")
    id("io.micronaut.aot")
}

dependencies {
    compileOnly("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut:micronaut-jackson-databind") // TODO: decide between Jackson or Micronaut Serde
    implementation("jakarta.annotation:jakarta.annotation-api")
    runtimeOnly("org.yaml:snakeyaml")
    runtimeOnly("ch.qos.logback:logback-classic")
}
