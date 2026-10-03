package com.github.jmlb23.data.kafka_streams

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.schema
import com.github.jmlb23.data.kafka_streams.utils.BlueSkyMessageLocalSerde
import com.github.jmlb23.data.kafka_streams.utils.Message
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Inject
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger



@ApplicationScoped
class AvroStreamTopologyProvider(
    val json: Json,
    val logger: Logger,
    @ConfigProperty(name = "data-lake.kafka-streams.raw.topic")
    val rawLandingTopicName: String,
    @ConfigProperty(name = "data-lake.kafka-streams.enriched.topic")
    val enrichedTopicName: String,
) {

    @Produces
    fun buildTopology(): Topology {
        val builder = StreamsBuilder()
        val customSerde = BlueSkyMessageLocalSerde()

        builder
            .stream(rawLandingTopicName, Consumed.with(Serdes.String(), Serdes.String()))
            .map { _, value ->
                val replaceDollar = value.replace($$"$", "")
                val jsonParsed = json.decodeFromString<Message>(replaceDollar)
                val key = jsonParsed.payload?.did ?: "UNKNOWN"
                KeyValue(key, jsonParsed)
            }
            .to(enrichedTopicName, Produced.with(Serdes.String(), customSerde))
        return builder.build()
    }
}
