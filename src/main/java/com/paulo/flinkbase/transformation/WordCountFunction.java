package com.paulo.flinkbase.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.transformation
 * @date:2020/4/18
 */
public class WordCountFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {
    @Override
    public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = str.split("\\W+");
        for (String word : words) {
            collector.collect(new Tuple2<>(word, 1));
        }
    }
}
