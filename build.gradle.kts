plugins {
    `java-library`
    kotlin("jvm")
    kotlin("plugin.serialization")
}

repositories {
    mavenCentral()
}

val integrationTest: SourceSet = sourceSets.create("integrationTest") {
    compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
}

configurations[integrationTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    implementation("org.slf4j:slf4j-api:2.0.20")
    implementation("com.aventrix.jnanoid:jnanoid:2.0.0")
    api("com.github.kagkarlsson:db-scheduler:16.12.0")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation("io.mockk:mockk:1.14.11")

    "integrationTestImplementation"("org.postgresql:postgresql:42.7.13")
    "integrationTestImplementation"("org.testcontainers:testcontainers-postgresql:2.0.5")
}


kotlin {
    jvmToolchain(25)
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against a Postgres Testcontainer (requires Docker)."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}
