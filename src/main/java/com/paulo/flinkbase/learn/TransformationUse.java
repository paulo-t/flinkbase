package com.paulo.flinkbase.learn;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/16
 */
public class TransformationUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", "127.0.0.1:9092");
        consumerConfig.setProperty("group.id", "flinkbase");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), consumerConfig);
        kafkaConsumer.setStartFromEarliest();

        DataStreamSource<String> messages = env.addSource(kafkaConsumer);
        messages.print();

        //单流操作->filter
        DataStream<String> filteredDataStream = messages.filter((FilterFunction<String>) str -> !StringUtils.isEmpty(str));
        filteredDataStream.print();

        //单流操作->map
        SingleOutputStreamOperator<String> mapDataStream = filteredDataStream.map((MapFunction<String, String>) str -> str + "");
        mapDataStream.print();

        //单流操作->flatMap
        DataStream<Tuple2<String, Integer>> flatMapDataStream = mapDataStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (str, collector) -> {
            String[] words = str.split("\\W+");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        });
        flatMapDataStream.print();

        //单流操作->keyBy将数据流转换成keyedStream
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMapDataStream.keyBy(0);

        //单流操作->reduce 增量操作 将keyedStream转换成为dataStream
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceDataStream = keyedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1));
        reduceDataStream.print();

        //Aggregations 一些常用操作对reduce的封装
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        sum.print();

        //单流操作->process 将keyedStream转换成为dataStream
        SingleOutputStreamOperator<Tuple2<String, Integer>> process = keyedStream.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //获取时间相关信息
                //context.timerService().currentWatermark();
                //花去keyedState
                //getRuntimeContext().getListState();
                //旁侧输出
                //context.output();
                //数据处理
                collector.collect(new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + 1));
            }
        });
        process.print();

        env.execute(TransformationUse.class.getSimpleName());
    }
}
