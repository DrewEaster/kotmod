plugins {
    id("kotmod.published")
}

description = "Querty blocking JDBC engine: runs Querty statements on a java.sql.Connection."

dependencies {
    api(project(":querty"))

    "integrationTestImplementation"("org.testcontainers:testcontainers-postgresql:2.0.5")
    "integrationTestImplementation"("org.postgresql:postgresql:42.7.13")
}
