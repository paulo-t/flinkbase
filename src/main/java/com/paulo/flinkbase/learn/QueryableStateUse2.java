package com.paulo.flinkbase.learn;

import com.paulo.flinkbase.transformation.WordCountFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/18
 */
public class QueryableStateUse2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource = env.socketTextStream("127.0.0.1", 9000);
        DataStream<Tuple2<String, Integer>> flatMapSource = socketSource.flatMap(new WordCountFunction()).returns(new TypeHint<Tuple2<String, Integer>>() {
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> wordCount = flatMapSource
                .keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Long>>() {
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
                        //初始化单词数量的状态数据
                        ValueStateDescriptor<Tuple2<String, Long>> valueStateDescriptor = new ValueStateDescriptor("wordCount", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        }));
                        //将keyed State设置成为可查询的
                        valueStateDescriptor.setQueryable("handle-way");
                        wordCount = getRuntimeContext().getState(valueStateDescriptor);
                    }
                });

        wordCount.print();

        env.execute(QueryableStateUse2.class.getSimpleName());
    }
}
