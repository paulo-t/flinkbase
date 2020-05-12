package com.paulo.flinkbase.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

public class DataWithTime extends RichParallelSourceFunction<Tuple2<String, Long>> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
        Object checkpointLock = sourceContext.getCheckpointLock();
        while (isRunning) {
            synchronized (checkpointLock) {
                Tuple2 tuple = new Tuple2<>(UUID.randomUUID().toString().replace("-", ""), 1L/*System.currentTimeMillis()*/);
                sourceContext.collect(tuple);
                sourceContext.collect(tuple);
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
