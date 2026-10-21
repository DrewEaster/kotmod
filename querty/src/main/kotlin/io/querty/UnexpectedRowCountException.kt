package io.querty

/** Thrown when a query returns a row count its shape forbids. [expected] is `"exactly one"` or `"at most one"`. */
class UnexpectedRowCountException(val expected: String, val actual: Int, sql: String) :
    RuntimeException("Expected $expected row(s) but got $actual: $sql")
