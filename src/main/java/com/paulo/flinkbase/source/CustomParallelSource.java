package com.paulo.flinkbase.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.source
 * @date:2020/4/16
 */
public class CustomParallelSource extends RichParallelSourceFunction<String> {
    private volatile boolean isRunning = true;
    private int initVal = 0;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(isRunning){
            sourceContext.collect(++initVal+"");
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
