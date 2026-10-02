package com.github.jmlb23.data.kafka_streams.utils

import kotlinx.serialization.*
import kotlinx.serialization.json.*
import kotlinx.serialization.encoding.*
import java.time.String

@Serializable
data class Message(
    @SerialName("\$type")
    val type: String? = null,

    val payload: Payload? = null
)

@Serializable
data class Payload(
    @SerialName("\$type")
    val type: String? = null,

    val cid: String? = null,
    val collection: String? = null,
    val did: String? = null,
    val operation: String? = null,
    val record: PayloadRecord? = null,
    val rev: String? = null,
    val rkey: String? = null,
    val seq: Long? = null,
    val time: String? = null,
    val witnessedAt: String? = null
)

@Serializable
data class PayloadRecord(
    @SerialName("\$type")
    val type: String? = null,

    val createdAt: String? = null,
    val embed: Embed? = null,
    val langs: List<String>? = null,
    val text: String? = null
)

@Serializable
data class Embed(
    @SerialName("\$type")
    val type: String? = null,

    val record: EmbedRecord? = null
)

@Serializable
data class EmbedRecord(
    val cid: String? = null,
    val uri: String? = null
)
