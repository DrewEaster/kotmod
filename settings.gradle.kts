pluginManagement {
    includeBuild("build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "kotmod-root"

include("kotmod", "kotmod-sqldelight", "kotmod-db-scheduler", "examples")
