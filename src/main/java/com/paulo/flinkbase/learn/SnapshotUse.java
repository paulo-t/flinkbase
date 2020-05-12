package com.paulo.flinkbase.learn;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;
import java.util.List;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/29
 */
public class SnapshotUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(20000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("127.0.0.1", 9000);
        stringDataStreamSource.map(new SnapshotUseMap()).print();
        env.execute(SnapshotUse.class.getSimpleName());
    }

    private static class SnapshotUseMap extends RichMapFunction<String,Tuple2<String,Integer>> implements CheckpointedFunction{

        private ListState<Tuple2<String,Integer>> listState;

        List<Tuple2<String,Integer>> values = Lists.newArrayList();

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println(Thread.currentThread().getId()+" start snapshot...");
            listState.clear();
            for (Tuple2<String, Integer> value : values) {
                listState.add(value);
            }
            System.out.println(Thread.currentThread().getId()+" snapshot end...");
            System.out.println(Thread.currentThread().getId()+" snapshot data:");
            Iterator<Tuple2<String, Integer>> iterator = listState.get().iterator();
            while (iterator.hasNext()){
                System.out.println(iterator.next());
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println(Thread.currentThread().getId()+" start initialize...");
            ListStateDescriptor<Tuple2<String,Integer>> listStateDescriptor = new ListStateDescriptor("listState", TypeInformation.of(new TypeHint<Tuple2<String,Integer>>() {
            }));
            listState = context.getOperatorStateStore().getListState(listStateDescriptor);
            if(context.isRestored()){
                Iterator<Tuple2<String, Integer>> iterator = listState.get().iterator();
                while (iterator.hasNext()){
                    Tuple2<String, Integer> next = iterator.next();
                    values.add(next);
                }
            }

            System.out.println(Thread.currentThread().getId()+" initialize end...");
        }

        @Override
        public Tuple2<String, Integer> map(String str) throws Exception {
            Tuple2<String, Integer> data = new Tuple2<>(str, 1);
            values.add(data);
            return data;
        }
    }
}
