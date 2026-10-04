package io.kotmod.postgres

import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class PostgresLeaderElectionIntegrationTest : IntegrationTest() {
    private val elections = mutableListOf<PostgresLeaderElection>()

    private fun election(
        name: String = "orders",
        connect: () -> Connection = dataSource::getConnection,
    ): PostgresLeaderElection =
        PostgresLeaderElection(connect, name, checkInterval = 200.milliseconds).also { elections += it }

    @AfterEach
    fun stopAll() =
        runBlocking {
            elections.forEach { it.stop() }
        }

    private fun advisoryLocksHeld(): Int =
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted").use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }

    @Test
    fun `is not leader before start and becomes leader after start`() =
        runBlocking {
            val a = election()
            assertFalse(a.isLeader())

            a.start()

            eventually(2.seconds) { a.isLeader() }
        }

    @Test
    fun `only one of two elections with the same name leads`() =
        runBlocking {
            val a = election().also { it.start() }
            val b = election().also { it.start() }

            eventually(2.seconds) { a.isLeader() || b.isLeader() }
            repeat(20) {
                assertEquals(1, listOf(a, b).count { it.isLeader() })
                delay(50)
            }
        }

    @Test
    fun `elections with different names lead independently`() =
        runBlocking {
            val orders = election("orders").also { it.start() }
            val invoices = election("invoices").also { it.start() }

            eventually(2.seconds) { orders.isLeader() && invoices.isLeader() }
        }

    @Test
    fun `stop releases the lock`() =
        runBlocking {
            val a = election().also { it.start() }
            eventually(2.seconds) { a.isLeader() }
            assertEquals(1, advisoryLocksHeld())

            a.stop()

            assertFalse(a.isLeader())
            assertEquals(0, advisoryLocksHeld())
        }

    @Test
    fun `stopping the leader hands over to the other election`() =
        runBlocking {
            val a = election().also { it.start() }
            eventually(2.seconds) { a.isLeader() }
            val b = election().also { it.start() }
            delay(500)
            assertFalse(b.isLeader())

            a.stop()

            eventually(1.seconds) { b.isLeader() }
        }

    @Test
    fun `a leader whose session the server ends steps down and the other takes over`() =
        runBlocking {
            val a = election().also { it.start() }
            eventually(2.seconds) { a.isLeader() }
            val b = election().also { it.start() }

            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted",
                    )
                }
            }

            eventually(2.seconds) { b.isLeader() && !a.isLeader() }

            // a has rejoined as a follower: it takes over once b stops.
            b.stop()
            eventually(2.seconds) { a.isLeader() }
        }

    @Test
    fun `connect failures keep it a follower until a connection succeeds`() =
        runBlocking {
            val attempts = AtomicInteger()
            val a =
                election(connect = {
                    if (attempts.incrementAndGet() <= 3) error("database unavailable")
                    dataSource.connection
                })

            a.start()

            eventually(3.seconds) { a.isLeader() }
            assertTrue(attempts.get() >= 4)
        }
}
