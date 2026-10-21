package io.querty.codegen

enum class Shape { ONE, OPTIONAL, MANY, AFFECTED }

data class TypeDecl(val kotlinType: String, val isEnum: Boolean)

/**
 * SQL text as it appears in [file]: [firstLine] is the 1-based line of the first line of [text], and
 * later lines follow the file's line breaks, so an offset in [text] maps back to a file line.
 */
data class SourceText(val file: String, val firstLine: Int, val text: String)

data class QueryDef(
    val name: String,
    val shape: Shape,
    val doc: String?,
    val types: Map<String, TypeDecl>,
    val imports: List<String>,
    val notNull: Set<String>,
    val nullable: Set<String>,
    val body: SourceText,
)

data class SnippetDef(val name: String, val body: SourceText)

data class ParsedFile(
    val path: String,
    val fileImports: List<String>,
    val queries: List<QueryDef>,
    val snippets: List<SnippetDef>,
)
