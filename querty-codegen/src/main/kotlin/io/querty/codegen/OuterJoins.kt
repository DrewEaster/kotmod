package io.querty.codegen

import io.querty.SqlLexer

sealed interface NullableTables {
    /** Lower-cased table names (not aliases) on the nullable side of an outer join. */
    data class Known(val tables: Set<String>) : NullableTables

    /** There is an outer join, but the FROM clause is too complex to attribute it to tables. */
    data object Unknown : NullableTables
}

/** Finds which tables of a query's top-level FROM clause an outer join can make null. */
object OuterJoins {
    private val fromEnd = setOf(
        "where", "group", "order", "having", "limit", "offset", "union", "intersect", "except", "window", "fetch",
        "for", "returning",
    )

    private class Token(val text: String, val isWord: Boolean)

    fun nullableTables(sql: String): NullableTables {
        val from = fromClause(tokenize(sql))
        val nullable = mutableSetOf<String>()
        val tables = mutableListOf<String>()
        var complex = false
        var outer = false
        var expectTable = true
        var inCondition = false
        var kind: String? = null
        var depth = 0
        var i = 0
        while (i < from.size) {
            val token = from[i++]
            val word = if (token.isWord) token.text.lowercase() else null
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
                word == "left" || word == "right" || word == "full" -> kind = word
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
                    if (kind != null) outer = true
                    kind = null
                    tables += name
                }
                else -> {}
            }
        }
        if (!outer) return NullableTables.Known(emptySet())
        return if (complex) NullableTables.Unknown else NullableTables.Known(nullable)
    }

    /** The tokens of the first parenthesis-depth-0 FROM, up to the keyword that ends the clause. */
    private fun fromClause(tokens: List<Token>): List<Token> {
        var depth = 0
        var start = -1
        for ((index, token) in tokens.withIndex()) {
            if (token.text == "(") depth++
            else if (token.text == ")") depth--
            else if (depth == 0 && token.isWord && token.text.equals("from", ignoreCase = true)) {
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
            else if (depth == 0 && token.isWord && token.text.lowercase() in fromEnd) break
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
                    tokens += Token(sql.substring(i + 1, (after - 1).coerceAtLeast(i + 1)).replace("\"\"", "\""), true)
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
