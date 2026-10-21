package io.querty.codegen

import io.querty.PgType

/** A parameter of a query: [type] is the Postgres type, [decl] the app type it is declared as, if any. */
data class ParamModel(
    val sqlName: String,
    val kotlinName: String,
    val type: PgType,
    val baseType: BaseType,
    val decl: TypeDecl?,
    val nullable: Boolean,
)

/** A result column of a query, read at its index in [QueryModel.columns]. */
data class ColumnModel(
    val label: String,
    val kotlinName: String,
    val type: PgType,
    val baseType: BaseType,
    val decl: TypeDecl?,
    val nullable: Boolean,
)

/** Everything needed to emit one query: params in first-appearance order, and its file's and own imports. */
data class QueryModel(
    val def: QueryDef,
    val objectName: String,
    val rewritten: RewrittenQuery,
    val params: List<ParamModel>,
    val columns: List<ColumnModel>,
    val imports: List<String>,
)

/** Checks a described query against its directives and works out the Kotlin types of its params and columns. */
object QueryModelBuilder {
    private val enumBases = setOf("text", "varchar")

    fun build(def: QueryDef, rewritten: RewrittenQuery, description: Description, fileImports: List<String>): QueryModel {
        val file = def.body.file
        val errors = mutableListOf<QuertyError>()

        fun error(line: Int, message: String) {
            errors += QuertyError(file, line, message)
        }

        val paramTypes = rewritten.paramNames.mapIndexed { index, name ->
            val types = rewritten.refs.indices.filter { rewritten.refs[it] == index }.map { description.paramTypes[it] }.distinct()
            if (types.size > 1) {
                error(def.line, "Parameter '$name' is used as ${types.joinToString(" and ") { it.name }}; cast one use so they agree")
            }
            types.first()
        }

        val params = rewritten.paramNames.zip(paramTypes).mapNotNull { (name, type) ->
            val base = TypeMapping.base(type)
            if (base == null) {
                error(def.line, "Parameter '$name' has unsupported type ${type.name}; cast it in SQL (e.g. :$name::text)")
                return@mapNotNull null
            }
            val kotlinName = Naming.propertyName(name)
            if (!Naming.isValid(kotlinName)) {
                error(def.line, "Parameter '$name' needs a name that is a valid Kotlin name")
                return@mapNotNull null
            }
            ParamModel(name, kotlinName, type, base, def.types[name], nullable = name in def.nullable)
        }
        params.groupBy { it.kotlinName }.filterValues { it.size > 1 }.forEach { (kotlinName, same) ->
            val names = same.joinToString(" and ") { "'${it.sqlName}'" }
            error(def.line, "Parameters $names both become '$kotlinName' in Kotlin; rename one of them")
        }

        val described = description.columns
        if (def.shape == Shape.AFFECTED && described.isNotEmpty()) {
            error(def.line, "Query '${def.name}' is :affected but returns columns; use :one, :optional or :many")
        }
        if (def.shape != Shape.AFFECTED && described.isEmpty()) {
            val shape = def.shape.name.lowercase()
            error(def.line, "Query '${def.name}' is :$shape but returns no columns; use :affected for a statement without results")
        }
        described.groupBy { it.label }.filterValues { it.size > 1 }.keys.forEach {
            error(def.line, "Column label '$it' appears twice; alias one of them")
        }

        val outerJoined = OuterJoins.nullableTables(rewritten.numberedSql())
        val columns = described.mapNotNull { column ->
            val base = TypeMapping.base(column.type)
            if (base == null) {
                error(def.line, "Column '${column.label}' has unsupported type ${column.type.name}; cast it in SQL (e.g. ${column.label}::text)")
                return@mapNotNull null
            }
            val kotlinName = Naming.propertyName(column.label)
            if (!Naming.isValid(kotlinName)) {
                error(def.line, "Column '${column.label}' needs an alias that is a valid Kotlin name")
                return@mapNotNull null
            }
            val notNull = column.label in def.notNull ||
                (column.baseTable != null && column.baseNotNull && outerJoined is NullableTables.Known && column.baseTable !in outerJoined.tables)
            ColumnModel(column.label, kotlinName, column.type, base, def.types[column.label], nullable = !notNull)
        }.takeIf { def.shape != Shape.AFFECTED }.orEmpty()
        columns.distinctBy { it.label }.groupBy { it.kotlinName }.filterValues { it.size > 1 }.forEach { (kotlinName, same) ->
            val labels = same.joinToString(" and ") { "'${it.label}'" }
            error(def.line, "Columns $labels both become '$kotlinName' in Kotlin; alias one of them")
        }

        val labels = described.map { it.label }.toSet()
        val paramNames = rewritten.paramNames.toSet()
        for ((name, decl) in def.types) {
            if (name !in paramNames && name !in labels) {
                error(def.lineOf("type", name), "':type $name' names neither a parameter nor a result column")
            }
            if (decl.isEnum) {
                val type = paramTypes.getOrNull(rewritten.paramNames.indexOf(name)) ?: described.firstOrNull { it.label == name }?.type
                if (type != null && type.elementType.name !in enumBases) {
                    error(def.lineOf("type", name), "':type $name' is an enum but $name has type ${type.name}; enums need text or varchar")
                }
            }
        }
        def.notNull.filter { it !in labels }.forEach { error(def.lineOf("not-null", it), "':not-null $it' names no result column") }
        def.nullable.filter { it !in paramNames }.forEach { error(def.lineOf("nullable", it), "':nullable $it' names no parameter") }

        if (errors.isNotEmpty()) throw QuertyException(errors.sortedBy { it.line })
        return QueryModel(def, Naming.objectName(def.name), rewritten, params, columns, (fileImports + def.imports).distinct().sorted())
    }
}
