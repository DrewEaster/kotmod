plugins {
    id("kotmod.library")
}

description = "Querty code generator: reads .sql files, asks Postgres to describe them and emits typed Kotlin."

dependencies {
    implementation(project(":querty"))
    implementation("org.testcontainers:testcontainers-postgresql:2.0.5")
    implementation("org.postgresql:postgresql:42.7.13")
}
