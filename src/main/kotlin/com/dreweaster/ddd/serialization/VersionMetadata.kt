package com.dreweaster.ddd.serialization

/** One step in an event type's history: the class name it was stored under at a given schema [version]. */
data class VersionMetadata(
    val type: String,
    val version: Int,
)
