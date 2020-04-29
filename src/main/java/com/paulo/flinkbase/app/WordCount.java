package com.paulo.flinkbase.app;

import com.paulo.flinkbase.sink.BufferingSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Properties;

/**
 * 单词统计
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinklearn.app
 * @date:2020/4/14
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //2.配置数据源
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1",9000);
        //3.进行一系列转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            String[] words = s.split("\\W+");
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(0).sum(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = counts.keyBy(0).map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2;
            }
        });
        //4.配置输出
        //counts.writeAsText("output");
        map.addSink(new BufferingSink(2));
        Properties producerConfig = new Properties();
        FlinkKafkaPartitioner<Tuple2<String,Integer>> partitioner = new FlinkFixedPartitioner();
        partitioner.open(1,1);

        //String defaultTopic, KeyedSerializationSchema<IN> keyedSchema, FlinkKafkaPartitioner<IN> customPartitioner, KafkaSerializationSchema<IN> kafkaSchema, Properties producerConfig, FlinkKafkaProducer.Semantic semantic, int kafkaProducersPoolSize
        map.addSink(new FlinkKafkaProducer<Tuple2<String, Integer>>("test",new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),producerConfig, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        //5.提交执行
        env.execute(WordCount.class.getSimpleName());
    }
}
