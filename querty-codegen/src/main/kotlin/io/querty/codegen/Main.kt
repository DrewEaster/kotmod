package io.querty.codegen

import java.nio.file.Path
import kotlin.io.path.isDirectory
import kotlin.system.exitProcess

private const val USAGE = "Usage: --sql-dir <dir> --schema <path> [--schema <path>...] --out <dir> [--image <image>] (generate|check)"

fun main(args: Array<String>) {
    exitProcess(run(args))
}

/** Runs the generator from command-line [args]: 0 on success, 1 on errors or differences, 2 on bad arguments. */
fun run(args: Array<String>): Int {
    val config = parseArgs(args) ?: return 2.also { System.err.println(USAGE) }
    if (!config.first.sqlDir.isDirectory()) return 2.also { System.err.println("Not a directory: ${config.first.sqlDir}") }
    return try {
        when (config.second) {
            "generate" -> {
                Generator.generate(config.first)
                println("Querty: generated into ${config.first.outDir}")
                0
            }
            else -> {
                val differences = Generator.check(config.first)
                differences.forEach(System.err::println)
                if (differences.isEmpty()) 0 else 1
            }
        }
    } catch (e: QuertyException) {
        e.errors.forEach(System.err::println)
        1
    }
}

private fun parseArgs(args: Array<String>): Pair<GeneratorConfig, String>? {
    var sqlDir: Path? = null
    val schema = mutableListOf<Path>()
    var outDir: Path? = null
    var image: String? = null
    var mode: String? = null
    var i = 0
    while (i < args.size) {
        val arg = args[i++]
        if (arg in setOf("--sql-dir", "--schema", "--out", "--image")) {
            val value = args.getOrNull(i++) ?: return null
            when (arg) {
                "--sql-dir" -> sqlDir = Path.of(value)
                "--schema" -> schema.add(Path.of(value))
                "--out" -> outDir = Path.of(value)
                else -> image = value
            }
        } else if (arg in setOf("generate", "check") && mode == null) {
            mode = arg
        } else {
            return null
        }
    }
    if (sqlDir == null || schema.isEmpty() || outDir == null || mode == null) return null
    val config = GeneratorConfig(sqlDir, schema, outDir)
    return (if (image == null) config else config.copy(image = image)) to mode
}
