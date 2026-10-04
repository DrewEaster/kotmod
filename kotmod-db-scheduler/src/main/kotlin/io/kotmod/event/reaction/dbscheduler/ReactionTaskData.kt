package io.kotmod.event.reaction.dbscheduler

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/** Where an ordered reaction sits in its aggregate's history, and the reaction id its instance id was built from. */
@Serializable
internal data class OrderingStamp(
    val key: String,
    val sequence: Long,
    val ordinal: Int,
    val onGiveUp: String,
    val reactionId: String,
)

/**
 * The data stored with each db-scheduler task instance, encoded as a JSON string: the serialized trigger, its
 * retry count and, for ordered reactions, the ordering stamp, whether it is parked blocking its aggregate, and how
 * many times in a row it has waited for an earlier reaction (which backs off its rechecks).
 */
@Serializable
internal data class ReactionTaskData(
    val trigger: String,
    val retryCount: Int,
    val ordering: OrderingStamp? = null,
    val blocked: Boolean = false,
    val waits: Int = 0,
) {
    fun encode(): String = Json.encodeToString(serializer(), this)

    companion object {
        fun decode(taskData: String): ReactionTaskData = Json.decodeFromString(serializer(), taskData)
    }
}
