package com.paulo.flinkbase.learn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * 状态使用
 *
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/16
 */
public class KeyStateUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketSource = env.socketTextStream("127.0.0.1", 9000);

        DataStream<Tuple2<String, Integer>> flatMapSource = socketSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (str, collector) -> {
            String[] words = str.split("\\W+");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMapSource.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordCount = keyedStream.flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Long>>() {
            //存放单词的数量
            private transient ValueState<Tuple2<String, Long>> wordCount;

            @Override
            public void flatMap(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple2<String, Long>> collector) throws Exception {
                if (Objects.isNull(wordCount.value())) {
                    wordCount.update(new Tuple2<>(stringIntegerTuple2.f0, 1L));
                } else {
                    wordCount.update(new Tuple2<>(stringIntegerTuple2.f0, wordCount.value().f1 + 1));
                }

                collector.collect(wordCount.value());
            }

            @Override
            public void open(Configuration parameters) {
                //ttl配置
                StateTtlConfig ttlConfig = StateTtlConfig
                        //state过期时间
                        .newBuilder(Time.seconds(1))
                        //在创建和写时更新state
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        //过期了就不返回转态
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        //过期了没有被清除仍然返回
                        //.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                        .build();

                //初始化单词数量的状态数据
                ValueStateDescriptor<Tuple2<String, Long>> valueStateDescriptor = new ValueStateDescriptor("wordCount", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                }));
                //过期配置
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                wordCount = getRuntimeContext().getState(valueStateDescriptor);
            }
        });

        wordCount.print();

        env.execute(KeyStateUse.class.getSimpleName());
    }
}
