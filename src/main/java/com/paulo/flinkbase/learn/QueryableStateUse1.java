package com.paulo.flinkbase.learn;

import com.paulo.flinkbase.utils.ParseUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.io.File;
import java.net.URL;
import java.util.Objects;

/**
 * 可查询的状态的使用-将 KeyedStream 转换为 QueryableStateStream，类似于 Sink，后续不能进行任何转换操作
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/18
 */
public class QueryableStateUse1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取资源路径
        //URL resource = BroadcastStateUse.class.getClassLoader().getResource("numberHandle.txt");
        //Path path = Path.fromLocalFile(new File(resource.toURI()));
        Path path = new Path("F:\\flink\\flinkbase\\src\\main\\resources\\numberHandle.txt");
        TextInputFormat textInputFormat = new TextInputFormat(path);

        //配置流
        DataStreamSource<String> configDataStream = env.readFile(textInputFormat, path.getPath(), FileProcessingMode.PROCESS_CONTINUOUSLY, 300000);

        //配置类型转换
        SingleOutputStreamOperator<Tuple2<String, Double>> tupleConfig = configDataStream.map((MapFunction<String, Tuple2<String, Double>>) str -> {
            String[] splits = str.split(" ");
            if (splits.length == 2) {
                return new Tuple2<>(splits[0], ParseUtils.ParseDouble(splits[1], 0D));
            }
            return null;
        }).returns(new TypeHint<Tuple2<String, Double>>() {
        });

        //有效的配置
        SingleOutputStreamOperator<Tuple2<String, Double>> validConfig = tupleConfig.filter((FilterFunction<Tuple2<String, Double>>) config -> !Objects.isNull(config));

        ValueStateDescriptor<Tuple2<String, Double>> valueStateDescriptor = new ValueStateDescriptor<Tuple2<String, Double>>("handle-way", TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
        }));

        //将配置转换成为可查询的状态,至此结束
        QueryableStateStream<String, Tuple2<String, Double>> configQueryableStateStream = validConfig.keyBy((KeySelector<Tuple2<String, Double>, String>) stringDoubleTuple2 -> stringDoubleTuple2.f0).asQueryableState("handle-way",valueStateDescriptor);

        env.execute(QueryableStateUse1.class.getSimpleName());
    }
}
