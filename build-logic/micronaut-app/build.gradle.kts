plugins {
    `kotlin-dsl`
}

dependencies {
    implementation(project(":kotlin"))
    implementation("com.github.johnrengelman:shadow:8.1.1")
    implementation("io.micronaut.gradle:micronaut-gradle-plugin:4.1.1")
    implementation("io.micronaut.gradle:micronaut-aot-plugin:4.1.1")
}
