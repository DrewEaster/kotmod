package io.kotmod.scheduling.dbscheduler

import io.kotmod.support.assertEveryTestRuns
import kotlin.test.Test

class DbSchedulerIntegrationTestMethodsRunTest {
    @Test
    fun `every test method in this source set returns void, so JUnit runs it`() {
        assertEveryTestRuns(this::class.java)
    }
}
