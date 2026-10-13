package com.github.jmlb23.data.flink


import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import com.github.jmlb23.data.core.Message
import com.github.jmlb23.data.flink.util.AvroSerializerWithoutSchemaRegistry

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.PrintSink
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.ParameterTool
import java.io.FileNotFoundException
import java.time.Duration


object Application {

    val stopWords = listOf(
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your",
        "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
        "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what",
        "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
        "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
        "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or",
        "because", "as", "until", "while", "of", "at", "by", "for", "with", "about",
        "against", "between", "into", "through", "during", "before", "after", "above", "below", "to",
        "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
        "further", "then", "once", "here", "there", "when", "where", "why", "how", "all",
        "any", "both", "each", "few", "more", "most", "other", "some", "such", "no",
        "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s",
        "t", "can", "will", "just", "don", "should", "now"
    )

    @JvmStatic
    fun main(vararg args: String): Unit {
        val sEnv = StreamExecutionEnvironment.getExecutionEnvironment()

        //dirty crap to access the resources folder and the properties file
        val res = Thread.currentThread().contextClassLoader.getResource("application.properties")

        val props = ParameterTool.fromPropertiesFile(res?.path ?: throw FileNotFoundException())

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
            .flatMap(object : FlatMapFunction<String?, String> {
                //it fucks my mind that flatMap isnt like flatMap :: Monad m => m a -> (a -> m b)
                override fun flatMap(value: String?, out: Collector<String>) {
                    value?.split(' ')?.forEach { out.collect(it.trim().lowercase()) }
                }
            })
            .filter {  it !in stopWords }
            .map(object : MapFunction<String?, Pair<String?, Long>> {
                override fun map(value: String?): Pair<String?, Long> {
                    return Pair(value, 1L)
                }
            })
            .keyBy { it.first }
            .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
            .reduce { acc, new -> Pair(acc.first, acc.second + 1) }
            .windowAll(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
            .process(object : ProcessAllWindowFunction<Pair<String?, Long>, Pair<String?, Long>, TimeWindow>() {
                override fun process(
                    context: ProcessAllWindowFunction<Pair<String?, Long>?, Pair<String?, Long>?, TimeWindow?>.Context?,
                    elements: Iterable<Pair<String?, Long>?>,
                    out: Collector<Pair<String?, Long>?>?
                ) {
                    elements.sortedByDescending { it?.second }.take(10).forEach { out?.collect(it) }
                }
            })

        groupByCid.sinkTo(PrintSink()).setParallelism(1)

        sEnv.execute()
    }
}
