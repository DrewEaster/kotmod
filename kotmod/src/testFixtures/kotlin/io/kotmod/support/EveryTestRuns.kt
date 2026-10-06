package io.kotmod.support

import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.toPath
import kotlin.test.assertTrue

/**
 * Fails if any method annotated with JUnit's [Test], in the compiled classes directory holding [anchor] (one source
 * set's test classes), doesn't return void. JUnit 5 silently skips such a method; the usual cause is an expression-bodied
 * `fun x() = runBlocking { … }` whose last expression isn't `Unit` (write `runBlocking<Unit>`).
 */
fun assertEveryTestRuns(anchor: Class<*>) {
    val root = anchor.protectionDomain.codeSource.location.toURI().toPath()
    val skipped = nonVoidTestMethods(root, anchor.classLoader)
    assertTrue(skipped.isEmpty(), "JUnit skips these @Test methods because they don't return void:\n" + skipped.joinToString("\n"))
}

private fun nonVoidTestMethods(
    root: Path,
    loader: ClassLoader,
): List<String> {
    val classNames =
        Files.walk(root).use { paths ->
            paths
                .filter { it.extension == "class" }
                .map { root.relativize(it).toString().removeSuffix(".class").replace(File.separatorChar, '.') }
                .toList()
        }
    return classNames.sorted().flatMap { name ->
        Class
            .forName(name, false, loader)
            .declaredMethods
            .filter { it.isAnnotationPresent(Test::class.java) && it.returnType != Void.TYPE }
            .map { "$name.${it.name} returns ${it.returnType.name}" }
    }
}
