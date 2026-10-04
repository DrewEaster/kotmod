plugins {
    id("kotmod.library")
}

dependencies {
    api(project(":kotmod"))
    api("com.github.kagkarlsson:db-scheduler:16.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.slf4j:slf4j-api:2.0.20")

    "integrationTestImplementation"(testFixtures(project(":kotmod")))
}
