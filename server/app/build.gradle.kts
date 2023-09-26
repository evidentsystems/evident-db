plugins {
    id("com.evidentdb.build.micronaut-app")
    id("com.evidentdb.build.kapt")
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
    implementation("io.micronaut.grpc:micronaut-grpc-server-runtime")
    implementation("io.micronaut.validation:micronaut-validation")
    implementation("io.micronaut.micrometer:micronaut-micrometer-registry-prometheus")
    implementation("io.micronaut:micronaut-management")
    implementation("io.micronaut:micronaut-http-server-netty")
    implementation("io.micronaut.kafka:micronaut-kafka")

    testImplementation("io.micronaut:micronaut-http-client")
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
