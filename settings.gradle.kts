pluginManagement {
    includeBuild("build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "kotmod-root"

include("kotmod", "querty","kotmod-sqldelight", "kotmod-db-scheduler", "examples")
