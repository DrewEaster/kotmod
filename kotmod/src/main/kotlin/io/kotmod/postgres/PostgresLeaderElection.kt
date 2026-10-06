package io.kotmod.postgres

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.Executor
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Leader election on a Postgres session advisory lock, for the `isLeader` parameter of
 * [io.kotmod.reaction.EventReactor] and [io.kotmod.contract.PublicEventContract].
 *
 * Elections with the same [name] compete for one lock; the winner is leader for as long as its connection lives.
 * Every `checkInterval` a follower tries to take the lock and the leader confirms its connection still answers.
 * [isLeader] also turns false when the last successful check is older than twice `checkInterval`, so a leader
 * whose checks hang stops claiming leadership. When Postgres ends the leader's session before the leader notices,
 * two nodes may both see themselves as leader for up to about one `checkInterval`.
 *
 * [connect] should open a direct connection outside any pool (one is held for the election's lifetime, and a new
 * one is opened after a failure); it doesn't work through PgBouncer in transaction mode.
 */
class PostgresLeaderElection internal constructor(
    private val connect: () -> Connection,
    val name: String,
    private val checkInterval: Duration,
    private val nanoTime: () -> Long,
) : AutoCloseable {
    constructor(
        connect: () -> Connection,
        name: String,
        checkInterval: Duration = 5.seconds,
    ) : this(connect, name, checkInterval, System::nanoTime)

    private val log = LoggerFactory.getLogger(PostgresLeaderElection::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val lockName = "kotmod-leader:$name"
    private val leaseNanos = (checkInterval * 2).inWholeNanoseconds
    private val queryTimeoutSeconds = maxOf(1, ((checkInterval.inWholeMilliseconds + 999) / 1000).toInt())
    private val networkTimeoutMillis = (checkInterval * 2).inWholeMilliseconds.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    private val abortExecutor = Executor { it.run() }

    @Volatile private var leading = false

    @Volatile private var lastConfirmedAt = 0L

    // Written by the loop; stop() reads it to release the lock, or to abort a call the loop is stuck in.
    @Volatile private var connection: Connection? = null
    private var failing = false
    private var job: Job? = null

    /** True while this election holds the lock and its last successful check is recent. Never queries. */
    fun isLeader(): Boolean = leading && nanoTime() - lastConfirmedAt <= leaseNanos

    /** Starts competing for leadership in the background; the first attempt runs immediately. */
    @Synchronized
    fun start() {
        check(job == null) { "Leader election '$name' is already started" }
        job =
            scope.launch {
                while (isActive) {
                    tick()
                    delay(checkInterval)
                }
            }
    }

    /** Stops competing, releases the lock if held, and closes the connection. */
    suspend fun stop() {
        val running = synchronized(this) { job.also { job = null } } ?: return
        val wasLeading = leading
        leading = false
        running.cancel()
        // Blocking JDBC calls ignore cancellation. The network timeout bounds them; if the loop is still stuck after
        // that, abort its connection (Postgres releases the lock when the session ends) instead of waiting longer.
        if (withTimeoutOrNull(checkInterval * 2) { running.join() } == null) {
            log.warn("Leader election for '{}' did not stop in time; abandoning its connection", name)
            connection?.let(::drop)
            connection = null
            return
        }
        leading = false
        val conn = connection ?: return
        connection = null
        try {
            conn.prepareStatement("SELECT pg_advisory_unlock(hashtextextended(?, 0))").use { stmt ->
                stmt.queryTimeout = queryTimeoutSeconds
                stmt.setString(1, lockName)
                stmt.executeQuery().close()
            }
            conn.close()
            if (wasLeading) log.info("Stepped down as leader for '{}'", name)
        } catch (ex: Exception) {
            log.warn("Could not release leadership of '{}' cleanly", name, ex)
            drop(conn)
        }
    }

    /** Blocking [stop], for shutdown hooks and `use {}`. */
    override fun close() = runBlocking { stop() }

    private suspend fun tick() {
        val conn = connection ?: openConnection() ?: return
        try {
            if (leading) {
                conn.querySingle("SELECT 1")
                lastConfirmedAt = nanoTime()
            } else if (conn.querySingle("SELECT pg_try_advisory_lock(hashtextextended(?, 0))", lockName) &&
                currentCoroutineContext().isActive
            ) {
                lastConfirmedAt = nanoTime()
                leading = true
                log.info("Became leader for '{}'", name)
            }
            recovered()
        } catch (ex: Exception) {
            if (leading) {
                leading = false
                log.warn("Lost leadership for '{}'", name, ex)
            } else {
                failed("Leader election check for '{}' failed", ex)
            }
            drop(conn)
            connection = null
        }
    }

    private suspend fun openConnection(): Connection? {
        val conn =
            try {
                connect()
            } catch (ex: Exception) {
                failed("Could not connect for leader election '{}'", ex)
                return null
            }
        if (!currentCoroutineContext().isActive) {
            // stop() gave up waiting while connect() was blocked.
            drop(conn)
            return null
        }
        try {
            conn.autoCommit = true
            // Bounds every call on this connection, so a stalled network fails a check instead of hanging it.
            conn.setNetworkTimeout(abortExecutor, networkTimeoutMillis)
            // Lets Postgres notice a vanished client within a few intervals and release the lock.
            conn.execute("SET tcp_keepalives_idle = $queryTimeoutSeconds")
            conn.execute("SET tcp_keepalives_interval = $queryTimeoutSeconds")
            conn.execute("SET tcp_keepalives_count = 3")
        } catch (ex: Exception) {
            failed("Could not set up the connection for leader election '{}'", ex)
            drop(conn)
            return null
        }
        connection = conn
        recovered()
        return conn
    }

    private fun Connection.execute(sql: String) {
        prepareStatement(sql).use { stmt ->
            stmt.queryTimeout = queryTimeoutSeconds
            stmt.execute()
        }
    }

    // Returns the first column of the single row as a boolean (`SELECT 1` reads as true).
    private fun Connection.querySingle(
        sql: String,
        vararg params: String,
    ): Boolean =
        prepareStatement(sql).use { stmt ->
            stmt.queryTimeout = queryTimeoutSeconds
            params.forEachIndexed { i, p -> stmt.setString(i + 1, p) }
            stmt.executeQuery().use { rs ->
                rs.next()
                rs.getBoolean(1)
            }
        }

    private fun failed(
        message: String,
        ex: Exception,
    ) {
        if (failing) {
            log.debug(message, name, ex)
        } else {
            failing = true
            log.warn(message, name, ex)
        }
    }

    private fun recovered() {
        if (failing) {
            failing = false
            log.info("Leader election for '{}' is connected again", name)
        }
    }

    // Abort first (it never blocks on a dead socket), then close so a pooled connection is handed back.
    private fun drop(conn: Connection) {
        try {
            conn.abort(abortExecutor)
        } catch (ex: Exception) {
            log.debug("Aborting the leader election connection for '{}' failed", name, ex)
        }
        try {
            conn.close()
        } catch (ex: Exception) {
            log.debug("Closing the leader election connection for '{}' failed", name, ex)
        }
    }
}
