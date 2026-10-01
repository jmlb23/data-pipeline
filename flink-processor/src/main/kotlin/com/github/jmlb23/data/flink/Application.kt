package com.github.jmlb23.data.flink

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.datastream.api.ExecutionEnvironment
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.PrintSink


object Application {

    @JvmStatic
    fun main(vararg args: String) {
        val sEnv = StreamExecutionEnvironment.getExecutionEnvironment()

        //TODO: remove hardcoded crap
        val kafkaSource = KafkaSource.builder<String>()
            .setBootstrapServers("localhost:9092")
            .setTopics("bluesky-jetstream")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(SimpleStringSchema())
            .build()

        val kafkaDStream = sEnv.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-bluesky")

        val dummyOperation = kafkaDStream.map { it }

        dummyOperation.sinkTo(PrintSink())

        sEnv.execute()
    }
}
