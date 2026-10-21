package io.querty.codegen

/** A problem in a `.sql` file, reported at the file line it belongs to. */
data class QuertyError(val file: String, val line: Int, val message: String) {
    override fun toString() = "$file:$line: $message"
}

class QuertyException(val errors: List<QuertyError>) : RuntimeException(errors.joinToString("\n"))
