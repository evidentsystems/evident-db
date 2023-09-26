plugins {
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.kapt")
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("io.micronaut.application") version "4.1.1"
    id("io.micronaut.aot") version "4.1.1"
}

version = "0.1.0-alpha-SNAPSHOT"
group = "com.evidentdb"

dependencies {
    implementation(libs.arrow.core)
    implementation(libs.kotlinx.coroutines)

    implementation(project(":adapters"))
    implementation(project(":domain"))
    implementation(project(":service"))
    implementation(project(":transactor"))

    implementation(libs.kafka.streams)
    implementation(libs.grpc.kotlin.stub)

    implementation("info.picocli:picocli")
    kapt("info.picocli:picocli-codegen")
    compileOnly("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut.grpc:micronaut-grpc-server-runtime")
    implementation("io.micronaut:micronaut-jackson-databind") // TODO: decide between Jackson or Micronaut Serde
    implementation("jakarta.annotation:jakarta.annotation-api")
    implementation("io.micronaut.validation:micronaut-validation")
    implementation("io.micronaut.micrometer:micronaut-micrometer-registry-prometheus")
    implementation("io.micronaut:micronaut-management")
    implementation("io.micronaut:micronaut-http-server-netty")
    implementation("io.micronaut.kafka:micronaut-kafka")
    runtimeOnly("org.yaml:snakeyaml")
    runtimeOnly("ch.qos.logback:logback-classic")

    testImplementation("io.micronaut:micronaut-http-client")
}

kotlin {
    jvmToolchain {
        languageVersion.set(
            JavaLanguageVersion.of(
                "${project.properties["java.version"]}"
            )
        )
    }
}

kapt {
    arguments {
        arg("project", "${project.group}/${project.name}")
    }
}

application {
    mainClass.set("com.evidentdb.app.CliKt")
    applicationDefaultJvmArgs = listOf("-Xmx4g", "-Xms4g")
}

micronaut {
    version.set(libs.versions.micronaut)
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("com.evidentdb.*")
    }
    aot {
        // Please review carefully the optimizations enabled below
        // Check https://micronaut-projects.github.io/micronaut-aot/latest/guide/ for more details
        optimizeServiceLoading = false
        convertYamlToJava = false
        precomputeOperations = true
        cacheEnvironment = true
        optimizeClassLoading = true
        deduceEnvironment = true
        optimizeNetty = true
    }
}

graalvmNative {
    toolchainDetection = false
    binaries {
        named("main") {
            imageName = "evidentdb"
        }
    }
}
