package com.github.jmlb23.data.kafka_streams.utils

import kotlinx.serialization.*
import kotlinx.serialization.json.*
import kotlinx.serialization.encoding.*

@Serializable
data class Message(
    val type: String? = null,
    val payload: Payload? = null
)

@Serializable
data class Payload(
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
    val type: String? = null,
    val createdAt: String? = null,
    val embed: Embed? = null,
    val langs: List<String>? = null,
    val text: String? = null
)

@Serializable
data class Embed(
    val type: String? = null,
    val external: External? = null,
    val record: EmbedRecord? = null
)

@Serializable
data class EmbedRecord(
    val cid: String? = null,
    val uri: String? = null
)


@Serializable
data class External(
    val uri: String? = null,
    val title: String? = null,
    val description: String? = null,
    val thumb: BlobRef? = null
)

@Serializable
data class BlobRef(
    val type: String? = null,
    val mimeType: String? = null,
    val size: Int? = null,
    val ref: LinkRef? = null
)

@Serializable
data class LinkRef(
    val link: String? = null
)
