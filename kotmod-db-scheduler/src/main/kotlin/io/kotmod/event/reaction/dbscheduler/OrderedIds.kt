package io.kotmod.event.reaction.dbscheduler

/** `<len(key)>:<key>#` — the length prefix keeps different keys' ranges apart whatever characters they contain. */
internal fun orderedKeyPrefix(key: String): String = "${key.length}:$key#"

/** A db-scheduler instance id that sorts by key, then sequence, then ordinal. */
internal fun orderedInstanceId(
    key: String,
    sequence: Long,
    ordinal: Int,
    reactionId: String,
): String = orderedKeyPrefix(key) + sequence.toString().padStart(19, '0') + "#" + ordinal.toString().padStart(4, '0') + "#" + reactionId
