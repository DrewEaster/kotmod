package io.querty.codegen

import io.querty.PlaceholderStyle
import io.querty.SqlTemplate

/**
 * A query with its snippets expanded and its `:name` references split out, in [SqlTemplate]'s form:
 * [refs]`[k]` indexes [paramNames], which are in first-appearance order.
 */
data class RewrittenQuery(val parts: List<String>, val refs: List<Int>, val paramNames: List<String>, val sourceMap: SourceMap) {
    fun numberedSql(): String = SqlTemplate(parts, refs).render(PlaceholderStyle.NUMBERED)

    fun positionalSql(): String = SqlTemplate(parts, refs).render(PlaceholderStyle.POSITIONAL)
}

/** Maps offsets in [RewrittenQuery.numberedSql] back to the `.sql` file and line they came from. */
class SourceMap internal constructor(private val segments: List<Segment>) {
    /**
     * Text from [start] in the numbered SQL came from [source] at [sourceStart]. A placeholder segment
     * doesn't advance: every offset in it maps to the parameter reference.
     */
    internal class Segment(val start: Int, val source: SourceText, val sourceStart: Int, val advances: Boolean)

    /** The file and 1-based line of the character at [numberedSqlOffset]. */
    fun locate(numberedSqlOffset: Int): Pair<String, Int> {
        val segment = segments.lastOrNull { it.start <= numberedSqlOffset } ?: segments.first()
        val offset = if (segment.advances) segment.sourceStart + (numberedSqlOffset - segment.start).coerceAtLeast(0) else segment.sourceStart
        return segment.source.file to segment.source.lineAt(offset)
    }
}

/**
 * Maps a 0-based [offset] into the SQL pgjdbc sends for [RewrittenQuery.positionalSql] (its k-th `?` sent as
 * `$k` and `??` as `?`), which is what Postgres error positions count in, back to a file and line.
 */
fun RewrittenQuery.locateServerOffset(offset: Int): Pair<String, Int> {
    var server = 0
    var numbered = 0
    parts.forEachIndexed { k, part ->
        if (offset < server + part.length) return sourceMap.locate(numbered + offset - server)
        server += part.length
        numbered += part.length
        if (k < refs.size) {
            val serverLength = "$${k + 1}".length
            if (offset < server + serverLength) return sourceMap.locate(numbered)
            server += serverLength
            numbered += "$${refs[k] + 1}".length
        }
    }
    return sourceMap.locate(numbered)
}

internal fun SourceText.lineAt(offset: Int): Int = firstLine + (0 until offset.coerceAtMost(text.length)).count { text[it] == '\n' }

/** Expands `{{snippet}}` references and turns `:name` references into parameters, outside strings and comments. */
object SqlRewriter {
    private val snippetName = Regex("[a-z][a-z0-9-]*")

    private class Output {
        val parts = mutableListOf<String>()
        val current = StringBuilder()
        val refs = mutableListOf<Int>()
        val paramNames = mutableListOf<String>()
        val segments = mutableListOf<SourceMap.Segment>()
        val errors = mutableListOf<QuertyError>()
        var length = 0

        fun startRun(source: SourceText, at: Int) {
            segments += SourceMap.Segment(length, source, at, advances = true)
        }

        fun text(s: String) {
            current.append(s)
            length += s.length
        }

        fun param(name: String, source: SourceText, at: Int) {
            parts += current.toString()
            current.clear()
            val index = paramNames.indexOf(name).takeIf { it >= 0 } ?: paramNames.size.also { paramNames += name }
            refs += index
            segments += SourceMap.Segment(length, source, at, advances = false)
            length += "$${index + 1}".length
        }
    }

    fun rewrite(query: QueryDef, snippets: Map<String, SnippetDef>): RewrittenQuery {
        val out = Output()
        expand(query.body, emptyList(), snippets, out)
        if (out.errors.isNotEmpty()) throw QuertyException(out.errors)
        return RewrittenQuery(out.parts + out.current.toString(), out.refs, out.paramNames, SourceMap(out.segments))
    }

    /** Emits [source] into [out]; [chain] is the snippets being expanded, outermost first. */
    private fun expand(source: SourceText, chain: List<String>, snippets: Map<String, SnippetDef>, out: Output) {
        val text = source.text
        out.startRun(source, 0)
        var i = 0
        while (i < text.length) {
            val skipped = SqlLexer.skipNonCode(text, i)
            if (skipped > i) {
                out.text(text.substring(i, skipped))
                i = skipped
                continue
            }
            val c = text[i]
            if (c == ':' && text.getOrNull(i - 1) != ':' && text.getOrNull(i + 1).let { it != null && (it in 'a'..'z' || it == '_') }) {
                var end = i + 2
                while (end < text.length && text[end].let { it in 'a'..'z' || it in '0'..'9' || it == '_' }) end++
                out.param(text.substring(i + 1, end), source, i)
                i = end
                out.startRun(source, i)
                continue
            }
            if (c == '{' && text.startsWith("{{", i)) {
                val close = text.indexOf("}}", i + 2)
                val name = if (close < 0) null else text.substring(i + 2, close).trim().takeIf { snippetName.matches(it) }
                if (name != null) {
                    val snippet = snippets[name]
                    when {
                        snippet == null -> out.errors += QuertyError(source.file, source.lineAt(i), "Unknown snippet '$name'")
                        name in chain -> out.errors += QuertyError(
                            source.file,
                            source.lineAt(i),
                            "Snippet cycle: ${(chain.dropWhile { it != name } + name).joinToString(" -> ")}",
                        )
                        else -> expand(snippet.body, chain + name, snippets, out)
                    }
                    i = close + 2
                    out.startRun(source, i)
                    continue
                }
            }
            out.text(c.toString())
            i++
        }
    }
}
