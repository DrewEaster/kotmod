plugins {
    id("kotmod.library")
    `java-test-fixtures`
}

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.slf4j:slf4j-api:2.0.20")
    implementation("com.aventrix.jnanoid:jnanoid:2.0.0")

    testFixturesApi(kotlin("test-junit5"))
    testFixturesApi("org.testcontainers:testcontainers-postgresql:2.0.5")
    testFixturesApi("org.postgresql:postgresql:42.7.13")
    testFixturesApi("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
}
