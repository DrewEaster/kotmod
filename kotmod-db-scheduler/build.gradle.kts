plugins {
    id("kotmod.published")
}

description = "Durable kotmod event reactions on db-scheduler."

dependencies {
    api(project(":kotmod"))
    api("com.github.kagkarlsson:db-scheduler:16.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.slf4j:slf4j-api:2.0.20")

    testImplementation(testFixtures(project(":kotmod")))
    "integrationTestImplementation"(testFixtures(project(":kotmod")))
}
