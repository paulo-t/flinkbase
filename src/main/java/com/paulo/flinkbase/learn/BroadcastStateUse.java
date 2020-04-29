package com.paulo.flinkbase.learn;

import com.paulo.flinkbase.source.CounterSource;
import com.paulo.flinkbase.utils.ParseUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.util.Objects;

public class BroadcastStateUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //数据流
        DataStreamSource<Long> counterSource = env.addSource(new CounterSource());

        //获取资源路径
        URL resource = BroadcastStateUse.class.getClassLoader().getResource("numberHandle.txt");
        Path path = Path.fromLocalFile(new File(resource.toURI()));
        TextInputFormat textInputFormat = new TextInputFormat(path);

        //广播状态
        MapStateDescriptor<String, Double> configStateDescriptor = new MapStateDescriptor("handle-way", Types.STRING, Types.DOUBLE);
        //配置流
        BroadcastStream<String> configBroadcastStream = env.readFile(textInputFormat, path.getPath(), FileProcessingMode.PROCESS_CONTINUOUSLY, 300000).broadcast(configStateDescriptor);

        //将数据流和广播流通过connect连接起来
        //如果非广播流是 keyed stream，需要实现 KeyedBroadcastProcessFunction
        //如果非广播流是 non-keyed stream，需要实现 BroadcastProcessFunction
        SingleOutputStreamOperator<Double> process = counterSource.connect(configBroadcastStream).process(new BroadcastProcessFunction<Long, String, Double>() {
            /**
             *处理非广播流数据的方法
             */
            @Override
            public void processElement(Long num, ReadOnlyContext readOnlyContext, Collector<Double> collector) throws Exception {
                //非广播方只能读取状态，Flink 中没有跨任务的通信机制，在一个任务实例中的修改不能在并行任务间传递
                ReadOnlyBroadcastState<String, Double> broadcastState = readOnlyContext.getBroadcastState(configStateDescriptor);
                //没有处理的配置时等待配置加载，时间根据配置文件加载的时间来定
                if (!broadcastState.immutableEntries().iterator().hasNext()) {
                    Thread.sleep(100);
                }

                Double odd = broadcastState.get("odd");
                Double even = broadcastState.get("even");

                if (num % 2 == 0) {
                    collector.collect(Objects.isNull(even) ? num : even * num);
                } else {
                    collector.collect(Objects.isNull(odd) ? num : odd * num);
                }
            }

            /**
             * 处理广播流中接收的数据元
             */
            @Override
            public void processBroadcastElement(String str, Context context, Collector<Double> collector) throws Exception {
                //广播方对广播状态具有读和写的权限,保证0 Broadcast state 在算子的所有并行实例中是相同的
                BroadcastState<String, Double> broadcastState = context.getBroadcastState(configStateDescriptor);
                String[] splits = str.split(" ");
                if (splits.length == 2) {
                    broadcastState.put(splits[0], ParseUtils.ParseDouble(splits[1], 0D));
                    System.out.println("配置更新成功，当前配置信息:" + broadcastState);
                }
            }
        });

        process.print();


        env.execute(BroadcastStateUse.class.getSimpleName());
    }
}
