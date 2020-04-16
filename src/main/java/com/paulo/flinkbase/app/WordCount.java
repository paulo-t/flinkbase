package com.paulo.flinkbase.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        //2.配置数据源
        DataStreamSource<String> text = env.readTextFile("F:/flink/student.txt");
        //3.进行一系列转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            String[] words = s.split("\\W+");
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(0).sum(1);
        //4.配置输出
        counts.writeAsText("output");
        //5.提交执行
        env.execute(WordCount.class.getSimpleName());
    }
}
