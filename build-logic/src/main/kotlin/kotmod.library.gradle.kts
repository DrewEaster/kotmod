plugins {
    `java-library`
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.plugin.serialization")
}

group = "io.kotmod"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(25)
}

val integrationTest: SourceSet =
    sourceSets.create("integrationTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }

configurations[integrationTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    "testImplementation"(kotlin("test"))
    "testImplementation"("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    "testImplementation"("io.mockk:mockk:1.14.11")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against a Postgres Testcontainer (requires Docker)."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.named("test"))
}
