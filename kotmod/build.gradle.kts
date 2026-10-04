plugins {
    id("kotmod.published")
    `java-test-fixtures`
}

description = "Domain-driven design on Postgres: aggregates, domain events and a transactional outbox."

// The test fixtures are for kotmod's own tests; keep them out of the published artifacts.
val javaComponent = components["java"] as AdhocComponentWithVariants
javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
// The publishing plugin adds the fixtures' sources variant late in configuration.
afterEvaluate {
    javaComponent.withVariantsFromConfiguration(configurations["testFixturesSourcesElements"]) { skip() }
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
