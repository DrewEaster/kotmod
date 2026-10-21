package io.querty.codegen

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.name
import kotlin.io.path.writeText
import kotlin.test.assertEquals

class SchemaScriptsTest {
    @TempDir
    lateinit var dir: Path

    private fun file(parent: Path, name: String): Path = parent.resolve(name).also { it.writeText("select 1;") }

    @Test
    fun `versioned scripts sort numerically and come before other files`() {
        file(dir, "V10__c.sql")
        file(dir, "V2__b.sql")
        file(dir, "V1_1__a.sql")
        file(dir, "seed.sql")
        file(dir, "notes.txt")

        assertEquals(
            listOf("V1_1__a.sql", "V2__b.sql", "V10__c.sql", "seed.sql"),
            SchemaScripts.ordered(listOf(dir)).map { it.name },
        )
    }

    @Test
    fun `dotted versions compare numerically`() {
        file(dir, "V1.10__b.sql")
        file(dir, "V1.2__a.sql")

        assertEquals(listOf("V1.2__a.sql", "V1.10__b.sql"), SchemaScripts.ordered(listOf(dir)).map { it.name })
    }

    @Test
    fun `explicit files keep their position relative to directories`() {
        val first = dir.resolve("a").also { it.createDirectories() }
        val second = dir.resolve("b").also { it.createDirectories() }
        file(first, "V2__x.sql")
        file(first, "V1__y.sql")
        val loose = file(dir, "zzz.sql")
        file(second, "V1__z.sql")

        assertEquals(
            listOf("V1__y.sql", "V2__x.sql", "zzz.sql", "V1__z.sql"),
            SchemaScripts.ordered(listOf(first, loose, second)).map { it.name },
        )
    }
}
