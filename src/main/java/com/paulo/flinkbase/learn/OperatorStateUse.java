package com.paulo.flinkbase.learn;

import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/16
 */
public class OperatorStateUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource = env.socketTextStream("127.0.0.1", 9000);



        env.execute(OperatorStateUse.class.getSimpleName());
    }
}
