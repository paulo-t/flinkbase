package com.paulo.flinkbase.learn;

import com.paulo.flinkbase.source.DataWithTime;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * window编程迷行
 * dataStream
 * .keyBy()
 * .window(windowAssigner)
 * .trigger()
 * .evictor()
 * .allowLateness()
 * .sideOutputLateData()
 * .reduce()/aggregate()/fold()/process()
 * .getSideOutput()
 */
public class WindowUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.addSource(new DataWithTime());

        tuple2DataStreamSource
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2.f0;
                    }
                })
                //.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //算子 增量:reduce,aggregate,fold 全量:process,process厉害的点在于可以结合增量算子计算也可以操作windowState
                /* .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                     @Override
                     public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                         return new Tuple2<>(stringLongTuple2.f0,stringLongTuple2.f1 + t1.f1) ;
                     }
                 });*/
                /*      .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                          @Override
                          //创建状态积累器
                          public Tuple2<String, Long> createAccumulator() {
                              return new Tuple2<>("",0L);
                          }

                          @Override
                          //积累状态
                          public Tuple2<String, Long> add(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> stringLongTuple22) {
                              return new Tuple2(stringLongTuple2.f0,stringLongTuple2.f1 + stringLongTuple22.f1);
                          }
                          //处理每个元素
                          @Override
                          public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
                              return stringLongTuple2;
                          }

                          //多个累加器合并
                          @Override
                          public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
                              return new Tuple2<>(stringLongTuple2.f0,stringLongTuple2.f1 + acc1.f1);
                          }
                      })*/
                /*.process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Tuple2<String,Long> result = new Tuple2<>("",0L);
                        Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
                        while(iterator.hasNext()){
                            Tuple2<String, Long> next = iterator.next();
                            result.f0 = next.f0;
                            result.f1 += next.f1;
                        }
                        collector.collect(result);
                    }
                })*/
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return new Tuple2<>(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Long>, Tuple4<String, Long, Long, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple4<String, Long, Long, Long>> collector) throws Exception {
                        Tuple4<String, Long, Long, Long> result = new Tuple4<>("", 0L, 0L, 0L);
                        Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> next = iterator.next();
                            result.f0 = next.f0;
                            result.f1 += next.f1;
                        }
                        result.f2 = context.window().getStart();
                        result.f3 = context.window().getEnd();
                        collector.collect(result);
                    }
                }).print();

        env.execute(WindowUse.class.getSimpleName());
    }
}
