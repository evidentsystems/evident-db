plugins {
    `kotlin-dsl`
}

dependencies {
    implementation(project(":kotlin"))
    implementation("org.jetbrains.kotlin:kotlin-allopen:1.9.22")
    implementation("com.google.devtools.ksp:com.google.devtools.ksp.gradle.plugin:1.9.22-1.0.16")
    implementation("com.github.johnrengelman:shadow:8.1.1")
    implementation("io.micronaut.gradle:micronaut-gradle-plugin:4.2.1")
    implementation("io.micronaut.gradle:micronaut-aot-plugin:4.2.1")
}
