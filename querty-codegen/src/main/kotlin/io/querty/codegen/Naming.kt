package io.querty.codegen

/** Turns names from `.sql` files into Kotlin identifiers and file names. */
object Naming {
    private val hardKeywords = setOf(
        "as", "break", "class", "continue", "do", "else", "false", "for", "fun", "if", "in", "interface", "is",
        "null", "object", "package", "return", "super", "this", "throw", "true", "try", "typealias", "typeof",
        "val", "var", "when", "while",
    )

    /** `find-order` becomes `FindOrder`. */
    fun objectName(kebab: String): String = escape(pascal(kebab))

    /** `customer_id` becomes `customerId`. */
    fun propertyName(snake: String): String = escape(pascal(snake).replaceFirstChar { it.lowercaseChar() })

    /** `shop/order_status.sql` becomes `OrderStatusQueries.kt`. */
    fun fileName(sqlPath: String): String =
        pascal(sqlPath.substringAfterLast('/').substringAfterLast('\\').removeSuffix(".sql")) + "Queries.kt"

    private fun pascal(name: String): String =
        name.split('-', '_').filter { it.isNotEmpty() }.joinToString("") { part -> part.replaceFirstChar { it.uppercaseChar() } }

    private fun escape(identifier: String) = if (identifier in hardKeywords) "`$identifier`" else identifier
}
