package com.paulo.flinkbase.learn;

import com.paulo.flinkbase.sink.BufferingSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/30
 */
public class StateBackendUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointInterval(500);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StateBackend stateBackend = new FsStateBackend("file:///F:/flink/data/checkpoint",true);
        env.setStateBackend(stateBackend);

        DataStreamSource<String> stringSource = env.socketTextStream("127.0.0.1", 9000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = stringSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = str.split("\\W+");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        mapStream.keyBy(0).addSink(new BufferingSink(1000));

        env.execute(StateBackendUse.class.getSimpleName());
    }
}
