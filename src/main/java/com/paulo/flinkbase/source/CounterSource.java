package com.paulo.flinkbase.source;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Collections;
import java.util.List;

public class CounterSource extends RichSourceFunction<Long> implements ListCheckpointed<Long> {
    private volatile boolean isRunning = true;

    private Long offset = 0L;

    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) {
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Long> list) {
        for (Long l : list) {
            offset = l;
        }
    }

    @Override
    public void run(SourceContext<Long> sourceContext) throws InterruptedException {
        final Object checkpointLock = sourceContext.getCheckpointLock();

        while (isRunning) {
            //加锁保证原子性，每个task都会用这把锁
            synchronized (checkpointLock) {
                sourceContext.collect(offset++);
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
