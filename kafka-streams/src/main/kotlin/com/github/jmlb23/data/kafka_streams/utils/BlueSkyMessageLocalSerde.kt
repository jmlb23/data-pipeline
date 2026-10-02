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

    override fun deserializer(): Deserializer<Message?> = object : Deserializer<Message?> {
        override fun deserialize(topic: String?, data: ByteArray?): Message? {
            return data?.let {
                Avro.decodeFromByteArray<Message>(Avro.schema<Message>(), it)
            }
        }
    }

    override fun serializer(): Serializer<Message?> = object : Serializer<Message?> {
        override fun serialize(topic: String?, data: Message?): ByteArray? {
            return data?.let {
                Avro.encodeToByteArray(Avro.schema<Message>(), data)
            }
        }
    }

}


