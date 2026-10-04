plugins {
    id("kotmod.library")
}

dependencies {
    implementation(project(":kotmod"))
    implementation(project(":kotmod-db-scheduler"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    "integrationTestImplementation"(testFixtures(project(":kotmod")))
}
