package io.querty.codegen

/** Splits a `.sql` file into named queries and snippets using its `-- :` header directives. */
object SqlFileParser {
    private val namePattern = Regex("[a-z][a-z0-9-]*")
    private val whitespace = Regex("\\s+")
    private const val SHAPES = "one of :one, :optional, :many, :affected"

    private class Item(val isSnippet: Boolean, val name: String, val line: Int) {
        var shape: Shape? = null
        val doc = mutableListOf<String>()
        val types = linkedMapOf<String, TypeDecl>()
        val imports = mutableListOf<String>()
        val notNull = linkedSetOf<String>()
        val nullable = linkedSetOf<String>()
        val directiveLines = linkedMapOf<String, Int>()
        val body = mutableListOf<String>()
        var bodyFirstLine = 0
        var broken = false
    }

    fun parse(path: String, text: String): ParsedFile {
        val errors = mutableListOf<QuertyError>()
        val fileImports = mutableListOf<String>()
        val queries = mutableListOf<QueryDef>()
        val snippets = mutableListOf<SnippetDef>()
        var current: Item? = null

        fun error(line: Int, message: String) {
            errors += QuertyError(path, line, message)
        }

        fun finish() {
            val item = current ?: return
            current = null
            if (item.broken) return
            val lines = item.body.toMutableList()
            var first = item.bodyFirstLine
            while (lines.isNotEmpty() && lines.first().isBlank()) {
                lines.removeAt(0)
                first++
            }
            while (lines.isNotEmpty() && lines.last().isBlank()) lines.removeAt(lines.size - 1)
            val sql = lines.joinToString("\n").trimEnd().removeSuffix(";").trimEnd()
            if (sql.isBlank()) {
                error(item.line, "${if (item.isSnippet) "Snippet" else "Query"} '${item.name}' has an empty body")
                return
            }
            val source = SourceText(path, first, sql)
            if (item.isSnippet) {
                snippets += SnippetDef(item.name, source)
            } else {
                val shape = item.shape ?: return
                queries += QueryDef(
                    item.name,
                    shape,
                    item.doc.takeIf { it.isNotEmpty() }?.joinToString("\n"),
                    item.types,
                    item.imports,
                    item.notNull,
                    item.nullable,
                    source,
                    item.line,
                    item.directiveLines,
                )
            }
        }

        fun start(isSnippet: Boolean, args: String, line: Int) {
            finish()
            val parts = args.split(whitespace).filter { it.isNotEmpty() }
            val name = parts.firstOrNull().orEmpty()
            if (!namePattern.matches(name)) {
                error(line, "Invalid name '$name'; names are lowercase kebab-case, like 'find-order'")
                current = Item(isSnippet, name, line).also { it.broken = true }
                return
            }
            val item = Item(isSnippet, name, line)
            val duplicate = if (isSnippet) snippets.any { it.name == name } else queries.any { it.name == name }
            if (duplicate) error(line, "Duplicate ${if (isSnippet) "snippet" else "query"} name '$name'")
            if (isSnippet) {
                if (parts.size > 1) error(line, "Snippet '$name' takes no arguments after its name")
            } else {
                val shapeText = parts.getOrNull(1)
                if (shapeText == null) {
                    error(line, "Query '$name' has no shape; expected $SHAPES")
                } else {
                    item.shape = Shape.entries.firstOrNull { ":${it.name.lowercase()}" == shapeText }
                    if (item.shape == null) error(line, "Unknown shape '$shapeText' for query '$name'; expected $SHAPES")
                }
                if (parts.size > 2) error(line, "Unexpected '${parts.drop(2).joinToString(" ")}' after the shape of query '$name'")
            }
            current = item
        }

        fun header(item: Item, keyword: String, args: String, line: Int) {
            if (item.isSnippet) {
                error(line, "Snippet '${item.name}' takes no directives; found ':$keyword'")
                return
            }
            when (keyword) {
                "doc" -> item.doc += args
                "type" -> {
                    val parts = args.split(whitespace).filter { it.isNotEmpty() }
                    if (parts.size !in 2..3 || (parts.size == 3 && parts[2] != "enum")) {
                        error(line, "Expected ':type <name> <KotlinType> [enum]'")
                    } else if (parts[0] in item.types) {
                        error(line, "Duplicate ':type' for '${parts[0]}'")
                    } else {
                        item.types[parts[0]] = TypeDecl(parts[1], parts.size == 3)
                        item.directiveLines["type ${parts[0]}"] = line
                    }
                }
                "import" -> if (args.isBlank()) error(line, "Expected ':import <fully.qualified.Name>'") else item.imports += args
                "not-null", "nullable" -> if (args.isBlank()) {
                    error(line, if (keyword == "not-null") "Expected ':not-null <column>'" else "Expected ':nullable <param>'")
                } else {
                    (if (keyword == "not-null") item.notNull else item.nullable) += args
                    item.directiveLines.putIfAbsent("$keyword $args", line)
                }
                else -> error(line, "Unknown directive ':$keyword'")
            }
        }

        text.split("\n").forEachIndexed { index, raw ->
            val line = index + 1
            val trimmed = raw.trim()
            val item = current
            if (trimmed.startsWith("-- :")) {
                val rest = trimmed.removePrefix("-- :")
                val keyword = rest.takeWhile { !it.isWhitespace() }
                val args = rest.removePrefix(keyword).trim()
                when {
                    keyword == "name" -> start(isSnippet = false, args, line)
                    keyword == "snippet" -> start(isSnippet = true, args, line)
                    item == null -> when (keyword) {
                        "import" -> if (args.isBlank()) error(line, "Expected ':import <fully.qualified.Name>'") else fileImports += args
                        "doc", "type", "not-null", "nullable" -> error(line, "Directive ':$keyword' must follow a ':name' line")
                        else -> error(line, "Unknown directive ':$keyword'")
                    }
                    item.bodyFirstLine != 0 ->
                        error(line, "Directive ':$keyword' is not allowed inside a body; directives go before the SQL")
                    else -> header(item, keyword, args, line)
                }
            } else {
                val ignorable = trimmed.isEmpty() || trimmed.startsWith("--")
                when {
                    item == null -> if (!ignorable) error(line, "SQL outside a query or snippet; start one with '-- :name' or '-- :snippet'")
                    item.bodyFirstLine != 0 -> item.body += raw
                    !ignorable -> {
                        item.bodyFirstLine = line
                        item.body += raw
                    }
                }
            }
        }
        finish()

        if (errors.isNotEmpty()) throw QuertyException(errors)
        return ParsedFile(path, fileImports, queries, snippets)
    }
}
