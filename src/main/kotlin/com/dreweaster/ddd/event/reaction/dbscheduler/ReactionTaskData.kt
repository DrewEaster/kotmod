package com.dreweaster.ddd.event.reaction.dbscheduler

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/** The data stored with each db-scheduler task instance: the serialized trigger and its retry count, encoded as a JSON string. */
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
