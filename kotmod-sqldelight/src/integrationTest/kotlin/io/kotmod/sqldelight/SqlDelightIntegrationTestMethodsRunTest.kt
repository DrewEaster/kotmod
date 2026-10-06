package io.kotmod.sqldelight

import io.kotmod.support.assertEveryTestRuns
import kotlin.test.Test

class SqlDelightIntegrationTestMethodsRunTest {
    @Test
    fun `every test method in this source set returns void, so JUnit runs it`() {
        assertEveryTestRuns(this::class.java)
    }
}
