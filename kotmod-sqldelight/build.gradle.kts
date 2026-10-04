plugins {
    id("kotmod.library")
}

dependencies {
    api(project(":kotmod"))
    api("app.cash.sqldelight:runtime:2.4.0")
    api("app.cash.sqldelight:jdbc-driver:2.4.0")

    "integrationTestImplementation"(testFixtures(project(":kotmod")))
}
