package com.github.jmlb23.data.kafka_streams

import com.github.avrokotlin.avro4k.Avro
import com.github.jmlb23.data.kafka_streams.utils.BlueSkyMessageLocalSerde
import com.github.jmlb23.data.kafka_streams.utils.CustomLocalSerde
import com.github.jmlb23.data.kafka_streams.utils.Message
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.jboss.logging.Logger

@ApplicationScoped
class AvroStreamTopologyProvider {
    @Produces
    fun buildTopology(): Topology {
        val builder = StreamsBuilder()

        val customSerde = BlueSkyMessageLocalSerde()

        builder
            .stream("topic", Consumed.with(Serdes.String(), Serdes.String()))
            .map { _, value ->
                val jsonParsed = Json.decodeFromString<Message>(value)
                val key = jsonParsed.payload?.did ?: "UNKNOWN"
                KeyValue(key, jsonParsed)
            }
            .to("avro-jetstream-bluesky", Produced.with(Serdes.String(), customSerde))
        return builder.build()
    }
}
