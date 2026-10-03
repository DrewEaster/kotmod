package com.dreweaster.ddd.event.reaction.dbscheduler

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * The payload stored in db-scheduler's `task_data` column. Always a plain [String] as far as
 * db-scheduler is concerned, so it works with whichever db-scheduler serializer the app configures.
 */
@Serializable
internal data class ReactionTaskData(
    val trigger: String,
    val retryCount: Int,
) {
    fun encode(): String = Json.encodeToString(serializer(), this)

    companion object {
        fun decode(taskData: String): ReactionTaskData = Json.decodeFromString(serializer(), taskData)
    }
}
