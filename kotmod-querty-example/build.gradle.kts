plugins {
    id("kotmod.library")
}

description = "Kotmod with Querty: an example app persisting a Kotmod domain through generated Querty queries."

val querty by configurations.creating

dependencies {
    implementation(project(":kotmod"))
    implementation(project(":querty"))
    implementation(project(":querty-jdbc"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    "integrationTestImplementation"(testFixtures(project(":kotmod")))

    querty(project(":querty-codegen"))
}

sourceSets.main {
    kotlin.srcDir("src/generated/querty/kotlin")
}

fun registerQuerty(
    name: String,
    mode: String,
    summary: String,
) = tasks.register<JavaExec>(name) {
    group = "querty"
    description = summary
    classpath = querty
    mainClass.set("io.querty.codegen.MainKt")
    workingDir = projectDir
    args(
        "--sql-dir",
        "src/main/sql",
        "--schema",
        "src/main/resources/db/migration",
        "--out",
        "src/generated/querty/kotlin",
        mode,
    )
}

registerQuerty("generateQuerty", "generate", "Generates Kotlin from the .sql query files (requires Docker).")
registerQuerty("checkQuerty", "check", "Fails if the committed generated Kotlin is out of date with the .sql query files (requires Docker).")

tasks.named("integrationTest") {
    dependsOn("checkQuerty")
}
