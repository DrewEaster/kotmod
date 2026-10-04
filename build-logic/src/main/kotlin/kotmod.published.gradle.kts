// A kotmod library module that is published to Maven Central.

plugins {
    id("kotmod.library")
    id("com.vanniktech.maven.publish")
}

mavenPublishing {
    publishToMavenCentral()
    // Sign when a key is configured (~/.gradle/gradle.properties or CI). Without one, publishToMavenLocal still
    // works for a dry run; Maven Central rejects unsigned uploads, so a missing key can't slip into a release.
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }
    coordinates(group.toString(), project.name, version.toString())

    pom {
        name.set(project.name)
        description.set(provider { project.description })
        inceptionYear.set("2026")
        url.set("https://github.com/DrewEaster/kotmod")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("DrewEaster")
                name.set("Drew Easter")
                url.set("https://github.com/DrewEaster")
            }
        }
        scm {
            url.set("https://github.com/DrewEaster/kotmod")
            connection.set("scm:git:git://github.com/DrewEaster/kotmod.git")
            developerConnection.set("scm:git:ssh://git@github.com/DrewEaster/kotmod.git")
        }
    }
}
