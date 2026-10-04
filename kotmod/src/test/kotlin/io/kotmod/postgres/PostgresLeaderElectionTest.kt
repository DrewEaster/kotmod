package io.kotmod.postgres

import io.kotmod.postgres.support.eventually
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

class PostgresLeaderElectionTest {
    // A connection on which every query returns a single `true` row: the lock is always granted.
    private fun grantingConnection(): Connection {
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns true
        every { rs.getBoolean(1) } returns true
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        return conn
    }

    @Test
    fun `lease expires when checks stop succeeding`() =
        runBlocking {
            var now = 0L
            val election = PostgresLeaderElection(::grantingConnection, "orders", 1.hours) { now }
            election.start()
            eventually(2.seconds) { election.isLeader() }

            // With a 1-hour interval no further check runs; time alone ends the lease.
            now += 2.hours.inWholeNanoseconds
            assertTrue(election.isLeader())
            now += 1
            assertFalse(election.isLeader())

            election.stop()
        }

    @Test
    fun `start twice throws`() =
        runBlocking {
            val election = PostgresLeaderElection(::grantingConnection, "orders", 1.hours) { 0L }
            election.start()

            assertFailsWith<IllegalStateException> { election.start() }

            election.stop()
        }

    @Test
    fun `can start again after stop`() =
        runBlocking {
            val election = PostgresLeaderElection(::grantingConnection, "orders", 1.hours) { 0L }
            election.start()
            eventually(2.seconds) { election.isLeader() }
            election.stop()
            assertFalse(election.isLeader())

            election.start()

            eventually(2.seconds) { election.isLeader() }
            election.stop()
        }

    @Test
    fun `forces autocommit on the lock connection`() =
        runBlocking {
            val conn = grantingConnection()
            val election = PostgresLeaderElection({ conn }, "orders", 1.hours) { 0L }
            election.start()
            eventually(2.seconds) { election.isLeader() }

            verify { conn.autoCommit = true }
            election.stop()
        }

    @Test
    fun `stop unlocks and closes the connection`() =
        runBlocking {
            val conn = grantingConnection()
            val election = PostgresLeaderElection({ conn }, "orders", 1.hours) { 0L }
            election.start()
            eventually(2.seconds) { election.isLeader() }

            election.stop()

            verify { conn.prepareStatement("SELECT pg_advisory_unlock(hashtextextended(?, 0))") }
            verify { conn.close() }
            assertFalse(election.isLeader())
        }
}
