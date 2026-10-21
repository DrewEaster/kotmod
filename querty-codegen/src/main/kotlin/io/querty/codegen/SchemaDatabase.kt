package io.querty.codegen

import org.postgresql.util.PSQLException
import org.testcontainers.postgresql.PostgreSQLContainer
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.readText

/** Orders schema scripts the way a migration tool would: versioned files by version, then the rest by name. */
object SchemaScripts {
    private val versioned = Regex("""V(\d+(?:[._]\d+)*)__.*\.sql""")

    /**
     * Expands directories to their `*.sql` files. Within a directory, `V<version>__<desc>.sql` files
     * come first, sorted by version (numeric parts compared as numbers), then the others by name.
     * Arguments keep their given order.
     */
    fun ordered(paths: List<Path>): List<Path> =
        paths.flatMap { path ->
            if (path.isDirectory()) {
                val files = path.listDirectoryEntries("*.sql").filter { !it.isDirectory() }
                val (withVersion, others) = files.partition { versionOf(it) != null }
                withVersion.sortedWith { a, b -> compareVersions(versionOf(a)!!, versionOf(b)!!) } + others.sortedBy { it.name }
            } else {
                listOf(path)
            }
        }

    private fun versionOf(path: Path): List<Int>? =
        versioned.matchEntire(path.name)?.groupValues?.get(1)?.split('.', '_')?.map { it.toInt() }

    private fun compareVersions(a: List<Int>, b: List<Int>): Int {
        for (i in 0 until maxOf(a.size, b.size)) {
            val c = (a.getOrElse(i) { 0 }).compareTo(b.getOrElse(i) { 0 })
            if (c != 0) return c
        }
        return 0
    }
}

/**
 * A throwaway Postgres built from the schema scripts. The container starts once and lives until [close],
 * so one generator run can describe many queries against it.
 */
class SchemaDatabase(image: String, scripts: List<Path>) : AutoCloseable {
    private val container = PostgreSQLContainer(image)
    val connection: Connection

    init {
        container.start()
        try {
            // Simple protocol sends each script as one query text, so error positions refer to the script.
            open("simple").use { setup -> scripts.forEach { run(setup, it) } }
            connection = open("extended").also { it.autoCommit = false }
        } catch (e: Throwable) {
            container.stop()
            throw e
        }
    }

    private fun open(mode: String): Connection =
        DriverManager.getConnection(
            container.jdbcUrl,
            Properties().apply {
                setProperty("user", container.username)
                setProperty("password", container.password)
                setProperty("preferQueryMode", mode)
            },
        )

    private fun run(setup: Connection, script: Path) {
        val text = script.readText()
        try {
            setup.createStatement().use { it.execute(text) }
        } catch (e: PSQLException) {
            val position = e.serverErrorMessage?.position ?: 0
            val line = if (position > 0) text.take(position - 1).count { it == '\n' } + 1 else 1
            throw QuertyException(listOf(QuertyError(script.toString(), line, e.serverErrorMessage?.message ?: e.message.orEmpty())))
        }
    }

    override fun close() {
        runCatching { connection.close() }
        container.stop()
    }
}
