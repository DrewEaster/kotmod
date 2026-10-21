package io.querty.codegen

/** Character-level SQL scanning: finds the spans where SQL text is not code. */
internal object SqlLexer {
    /**
     * If a string literal, quoted identifier, comment or dollar-quoted string starts at [i], returns the
     * index just past it (the end of [text] when it is unterminated); otherwise returns [i].
     */
    fun skipNonCode(text: String, i: Int): Int {
        val c = text[i]
        val next = text.getOrNull(i + 1)
        return when {
            c == '\'' -> skipQuoted(text, i + 1, '\'', backslashEscapes = false)
            (c == 'E' || c == 'e') && next == '\'' && !isIdentifierChar(text.getOrNull(i - 1)) ->
                skipQuoted(text, i + 2, '\'', backslashEscapes = true)
            c == '"' -> skipQuoted(text, i + 1, '"', backslashEscapes = false)
            c == '-' && next == '-' -> text.indexOf('\n', i).let { if (it < 0) text.length else it }
            c == '/' && next == '*' -> skipBlockComment(text, i)
            c == '$' && !isIdentifierChar(text.getOrNull(i - 1)) -> skipDollarQuoted(text, i)
            else -> i
        }
    }

    fun isIdentifierChar(c: Char?) = c != null && (c.isLetterOrDigit() || c == '_' || c == '$')

    /** Skips to just past the closing [quote], from [start] just after the opening one; a doubled quote is an escape. */
    private fun skipQuoted(text: String, start: Int, quote: Char, backslashEscapes: Boolean): Int {
        var j = start
        while (j < text.length) {
            val c = text[j]
            when {
                backslashEscapes && c == '\\' -> j += 2
                c == quote && text.getOrNull(j + 1) == quote -> j += 2
                c == quote -> return j + 1
                else -> j++
            }
        }
        return text.length
    }

    private fun skipBlockComment(text: String, start: Int): Int {
        var depth = 0
        var j = start
        while (j < text.length) {
            when {
                text.startsWith("/*", j) -> {
                    depth++
                    j += 2
                }
                text.startsWith("*/", j) -> {
                    depth--
                    j += 2
                    if (depth == 0) return j
                }
                else -> j++
            }
        }
        return text.length
    }

    /** `$$...$$` or `$tag$...$tag$`; a `$` that does not open a tag (like `$1`) is code. */
    private fun skipDollarQuoted(text: String, start: Int): Int {
        var j = start + 1
        if (j < text.length && (text[j].isLetter() || text[j] == '_')) {
            while (j < text.length && (text[j].isLetterOrDigit() || text[j] == '_')) j++
        }
        if (text.getOrNull(j) != '$') return start
        val tag = text.substring(start, j + 1)
        val end = text.indexOf(tag, j + 1)
        return if (end < 0) text.length else end + tag.length
    }
}
