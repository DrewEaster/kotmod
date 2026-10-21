package io.querty

enum class PlaceholderStyle {
    /** `$1`, `$2`, ... A repeated parameter reuses its number. */
    NUMBERED,

    /** `?`. A repeated parameter is bound once per occurrence. */
    POSITIONAL,
}

/**
 * SQL text split around parameter references. [refs]`[k]` is the 0-based index into the
 * statement's params of the reference between `parts[k]` and `parts[k + 1]`.
 */
class SqlTemplate(val parts: List<String>, val refs: List<Int>) {
    init {
        require(parts.size == refs.size + 1) { "Expected ${refs.size + 1} parts for ${refs.size} refs but got ${parts.size}" }
    }

    fun render(style: PlaceholderStyle): String = buildString {
        parts.forEachIndexed { k, part ->
            append(if (style == PlaceholderStyle.POSITIONAL) part.replace("?", "??") else part)
            if (k < refs.size) {
                if (style == PlaceholderStyle.NUMBERED) append('$').append(refs[k] + 1) else append('?')
            }
        }
    }

    /** The params in the order the placeholders of [style] consume them. */
    fun bindOrder(style: PlaceholderStyle, params: List<Param>): List<Param> = when (style) {
        PlaceholderStyle.NUMBERED -> params
        PlaceholderStyle.POSITIONAL -> refs.map { params[it] }
    }
}
