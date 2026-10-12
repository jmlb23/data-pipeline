package com.github.jmlb23.data.flink


import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import com.github.jmlb23.data.core.Message
import com.github.jmlb23.data.flink.util.AvroSerializerWithoutSchemaRegistry
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.v2.ValueState
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.connector.sink2.WriterInitContext
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.datastream.api.ExecutionEnvironment
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.PrintSink
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.ParameterTool
import java.time.Duration


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
            .map { it.payload?.record?.text }
            .flatMap(object : FlatMapFunction<String?, String?> {
                //it fucks my mind that flatMap isnt like flatMap :: Monad m => m a -> (a -> m b)
                override fun flatMap(value: String?, out: Collector<String?>) {
                    value?.split(' ')?.forEach { out.collect(it) }
                }
            })
            .map(object : MapFunction<String?, Pair<String?, Long>> {
                override fun map(value: String?): Pair<String?, Long> {
                    return Pair(value, 1L)
                }
            })
            .keyBy { it.first }
            .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
            .reduce { acc, new -> Pair(acc.first, acc.second + 1) }

        groupByCid.sinkTo(PrintSink())

        sEnv.execute()
    }
}
