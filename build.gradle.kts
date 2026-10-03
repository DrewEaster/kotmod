plugins {
    `java-library`
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("app.cash.sqldelight") version "2.4.0"
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
    implementation("app.cash.sqldelight:runtime:2.4.0")
    implementation("app.cash.sqldelight:jdbc-driver:2.4.0")
    implementation("org.slf4j:slf4j-api:2.0.20")
    implementation("com.aventrix.jnanoid:jnanoid:2.0.0")
    api("com.github.kagkarlsson:db-scheduler:16.12.0")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation("io.mockk:mockk:1.14.11")

    "integrationTestImplementation"("org.postgresql:postgresql:42.7.13")
    "integrationTestImplementation"("org.testcontainers:testcontainers-postgresql:2.0.5")
}

sqldelight {
    databases {
        create("InfrastructureDatabase") {
            packageName.set("com.dreweaster.infrastructureservices.db.sqldelight")
            srcDirs("src/main/sqldelight/global")
            dialect("app.cash.sqldelight:postgresql-dialect:2.4.0")
        }
    }
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
