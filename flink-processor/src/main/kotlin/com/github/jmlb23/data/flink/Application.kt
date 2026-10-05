package com.github.jmlb23.data.flink


import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import com.github.jmlb23.data.core.Message
import com.github.jmlb23.data.flink.util.AvroSerializerWithoutSchemaRegistry
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.connector.sink2.WriterInitContext
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.datastream.api.ExecutionEnvironment
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.PrintSink
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction
import org.apache.flink.util.ParameterTool


object Application {

    @JvmStatic
    fun main(vararg args: String): Unit {
        val sEnv = StreamExecutionEnvironment.getExecutionEnvironment()

        //dirty crap to access the resources folder and the properties file
        val res = Thread.currentThread().contextClassLoader.getResource("application.properties")

        val props = ParameterTool.fromPropertiesFile(res.path)

        val schema = Avro.schema(Message.serializer())

        val kafkaSource = KafkaSource.builder<Message>()
            .setBootstrapServers(props.get("kafka.bootstrap"))
            .setTopics(props.get("kafka.topic"))
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                AvroSerializerWithoutSchemaRegistry()
            )
            .build()

        val kafkaDStream = sEnv.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-bluesky")

        val groupByCid = kafkaDStream
            .filter { it != null }
            .map { 1L }
            .keyBy { "COUNT" }
            .reduce { acc, new -> acc + new }

        groupByCid.sinkTo(PrintSink())

        sEnv.execute()
    }
}
