package com.github.jmlb23.data.flink.util

import com.github.avrokotlin.avro4k.Avro
import com.github.jmlb23.data.core.Message
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

class AvroSerializerWithoutSchemaRegistry : DeserializationSchema<Message> {

    override fun getProducedType(): TypeInformation<Message?>? {
        return TypeExtractor.getForClass(Message::class.java)
    }

    override fun deserialize(message: ByteArray?): Message? {
        return message?.let{
            Avro.decodeFromByteArray(Message.serializer(), it)
        }
    }

    override fun isEndOfStream(nextElement: Message?): Boolean {
       return false 
    }
}


