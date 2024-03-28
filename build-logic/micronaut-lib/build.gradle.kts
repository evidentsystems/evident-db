plugins {
    `kotlin-dsl`
}

dependencies {
    implementation(project(":kotlin"))
    implementation("org.jetbrains.kotlin:kotlin-allopen:1.9.23")
    implementation("com.google.devtools.ksp:com.google.devtools.ksp.gradle.plugin:1.9.23-1.0.19")
    implementation("io.micronaut.gradle:micronaut-gradle-plugin:4.3.4")
}
