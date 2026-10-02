package com.github.jmlb23.data.flink

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.datastream.api.ExecutionEnvironment
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.PrintSink
import org.apache.flink.util.ParameterTool


object Application {

    @JvmStatic
    fun main(vararg args: String) {
        val sEnv = StreamExecutionEnvironment.getExecutionEnvironment()

        //dirty crap to access the resources folder and the properties file
        val res = Thread.currentThread().contextClassLoader.getResource("application.properties")

        val props = ParameterTool.fromPropertiesFile(res.path)

        //TODO: remove hardcoded crap
        val kafkaSource = KafkaSource.builder<String>()
            .setBootstrapServers(props.get("kafka.bootstrap"))
            .setTopics(props.get("kafka.topic"))
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
