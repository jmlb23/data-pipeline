package com.github.jmlb23.data.kafka_streams.utils

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.schema
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer


class BlueSkyMessageLocalSerde : Serde<Message> {
    val serializer = Message.serializer()

    override fun deserializer(): Deserializer<Message?> = { topic: String?, data: ByteArray? ->
        data?.let {
            Avro.decodeFromByteArray<Message>(serializer, it)
        }
    }

    override fun serializer(): Serializer<Message?> = { topic: String?, data: Message? ->
        data?.let {
            Avro.encodeToByteArray(serializer, data)
        }
    }

}


