package io.querty.codegen

import io.querty.SqlLexer

sealed interface NullableTables {
    /** Lower-cased table names (not aliases) on the nullable side of an outer join. */
    data class Known(val tables: Set<String>) : NullableTables

    /** There is an outer join, but the FROM clause is too complex to attribute it to tables. */
    data object Unknown : NullableTables
}

/**
 * Finds which tables of a query's top-level FROM clause an outer join can make null. ROLLUP, CUBE and GROUPING SETS
 * also null out grouped columns, so a query using any of them is Unknown.
 */
object OuterJoins {
    private val fromEnd = setOf(
        "where", "group", "order", "having", "limit", "offset", "union", "intersect", "except", "window", "fetch",
        "for", "returning",
    )

    private class Token(val text: String, val isWord: Boolean, val quoted: Boolean = false) {
        /** The lower-cased text if this is an unquoted word (a possible keyword), otherwise null. */
        val keyword: String? = if (isWord && !quoted) text.lowercase() else null
    }

    private val unsafeAnywhere = setOf("with", "union", "intersect", "except")

    fun nullableTables(sql: String): NullableTables {
        val all = tokenize(sql)
        if (all.indices.any { isGroupingSet(all, it) }) return NullableTables.Unknown
        val outerJoins = all.indices.count { isOuterJoin(all, it) }
        if (outerJoins == 0) return NullableTables.Known(emptySet())
        // Only a plain SELECT with one top-level FROM is attributed precisely; anything else is Unknown.
        if (all.first().keyword != "select") return NullableTables.Unknown
        if (all.any { it.keyword in unsafeAnywhere }) return NullableTables.Unknown
        if (all.indices.any { all[it].keyword == "distinct" && all.getOrNull(it + 1)?.keyword == "from" }) {
            return NullableTables.Unknown
        }
        val from = fromClause(all)
        val nullable = mutableSetOf<String>()
        val tables = mutableListOf<String>()
        var complex = false
        var outer = 0
        var expectTable = true
        var inCondition = false
        var kind: String? = null
        var depth = 0
        var i = 0
        while (i < from.size) {
            val token = from[i++]
            val word = token.keyword
            when {
                token.text == "(" -> {
                    if (depth == 0 && !inCondition) complex = true
                    depth++
                }
                token.text == ")" -> depth--
                depth > 0 -> {}
                token.text == "," -> {
                    expectTable = true
                    inCondition = false
                }
                word == "lateral" -> complex = true
                (word == "left" || word == "right" || word == "full") && isOuterJoin(from, i - 1) -> kind = word
                word == "only" && expectTable -> {}
                word == "join" -> {
                    expectTable = true
                    inCondition = false
                }
                (word == "on" || word == "using") && !expectTable -> inCondition = true
                expectTable && token.isWord -> {
                    // A possibly schema-qualified name; the table is its last part.
                    var name = token.text
                    while (from.getOrNull(i)?.text == "." && from.getOrNull(i + 1)?.isWord == true) {
                        name = from[i + 1].text
                        i += 2
                    }
                    name = name.lowercase()
                    expectTable = false
                    when (kind) {
                        "left" -> nullable += name
                        "right" -> nullable += tables
                        "full" -> {
                            nullable += name
                            nullable += tables
                        }
                    }
                    if (kind != null) outer++
                    kind = null
                    tables += name
                }
                else -> {}
            }
        }
        // Every outer join must have been seen at depth 0 of this FROM; otherwise one is hidden somewhere we do not look.
        return if (complex || outer != outerJoins) NullableTables.Unknown else NullableTables.Known(nullable)
    }

    /** True if token [i] is an unquoted `ROLLUP` or `CUBE`, or starts an unquoted `GROUPING SETS`. */
    private fun isGroupingSet(tokens: List<Token>, i: Int): Boolean = when (tokens[i].keyword) {
        "rollup", "cube" -> true
        "grouping" -> tokens.getOrNull(i + 1)?.keyword == "sets"
        else -> false
    }

    /** True if token [i] is `LEFT`/`RIGHT`/`FULL` followed by `JOIN` or `OUTER JOIN`, all unquoted. */
    private fun isOuterJoin(tokens: List<Token>, i: Int): Boolean {
        if (tokens[i].keyword !in setOf("left", "right", "full")) return false
        val next = tokens.getOrNull(i + 1)?.keyword
        return next == "join" || (next == "outer" && tokens.getOrNull(i + 2)?.keyword == "join")
    }

    /** The tokens of the first parenthesis-depth-0 FROM, up to the keyword that ends the clause. */
    private fun fromClause(tokens: List<Token>): List<Token> {
        var depth = 0
        var start = -1
        for ((index, token) in tokens.withIndex()) {
            if (token.text == "(") depth++
            else if (token.text == ")") depth--
            else if (depth == 0 && token.keyword == "from") {
                start = index + 1
                break
            }
        }
        if (start < 0) return emptyList()
        val clause = mutableListOf<Token>()
        depth = 0
        for (token in tokens.drop(start)) {
            if (token.text == "(") depth++
            else if (token.text == ")") depth--
            else if (depth == 0 && token.text == ";") break
            else if (depth == 0 && token.keyword in fromEnd) break
            clause += token
        }
        return clause
    }

    /** Words, quoted identifiers (unquoted, as words) and single punctuation characters; strings and comments dropped. */
    private fun tokenize(sql: String): List<Token> {
        val tokens = mutableListOf<Token>()
        var i = 0
        while (i < sql.length) {
            val c = sql[i]
            val after = SqlLexer.skipNonCode(sql, i)
            when {
                c == '"' && after > i -> {
                    tokens += Token(sql.substring(i + 1, (after - 1).coerceAtLeast(i + 1)).replace("\"\"", "\""), true, quoted = true)
                    i = after
                }
                after > i -> i = after
                c.isLetter() || c == '_' -> {
                    var j = i
                    while (j < sql.length && SqlLexer.isIdentifierChar(sql[j])) j++
                    tokens += Token(sql.substring(i, j), true)
                    i = j
                }
                c.isWhitespace() -> i++
                else -> {
                    tokens += Token(c.toString(), false)
                    i++
                }
            }
        }
        return tokens
    }
}
