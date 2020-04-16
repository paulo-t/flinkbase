package com.paulo.flinkbase.sink;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;

public class BufferingSink extends RichSinkFunction<Tuple2<String, Integer>> implements CheckpointedFunction {
    /**
     * 缓存数据的状态
     */
    private transient ListState<Tuple2<String, Integer>> checkpointState;

    /**
     * 每次最多能存储的数据量
     */
    private final int threshold;
    /**
     * 缓存数据的列表
     */
    private List<Tuple2<String, Integer>> bufferElement;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        bufferElement = Lists.newArrayList();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) {
        bufferElement.add(value);
        if (bufferElement.size() >= threshold) {
            for (Tuple2<String, Integer> element : bufferElement) {
                System.out.println("数据已经添加到外部:" + element);
            }
            bufferElement.clear();
        }
    }

    /**
     * 触发checkpoint时调用的方法
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointState.clear();
        for (Tuple2<String, Integer> element : bufferElement) {
            checkpointState.add(element);
        }
    }

    /**
     * 初始化方法或者从快照恢复时调用的方法
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //获取状态数据
        ListStateDescriptor<Tuple2<String,Integer>> listStateDescriptor = new ListStateDescriptor<Tuple2<String, Integer>>("buffered-element", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));
        checkpointState = context.getKeyedStateStore().getListState(listStateDescriptor);

        //恢复状态数据
        if(context.isRestored()){
            for (Tuple2<String, Integer> element : checkpointState.get()) {
                bufferElement.add(element);
            }
        }
    }
}
