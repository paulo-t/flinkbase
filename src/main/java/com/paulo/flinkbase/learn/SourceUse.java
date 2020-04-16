package com.paulo.flinkbase.learn;

import com.paulo.flinkbase.source.CustomParallelSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.learn
 * @date:2020/4/15
 */
public class SourceUse {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.读取文件信息作为数据源
        //获取资源路径
       /* URL resource = SourceUse.class.getClassLoader().getResource("user.txt");
        Path path = Path.fromLocalFile(new File(resource.toURI()));
        //获取实体类对应的pojo类型
        PojoTypeInfo<Student> pojoType = (PojoTypeInfo<Student>)TypeExtractor.createTypeInfo(Student.class);
        String[] fieldNames = new String[]{"id","name","course","score"};
        //输入的格式
        PojoCsvInputFormat<Student> inputFormat = new PojoCsvInputFormat(path,pojoType,fieldNames);
        DataStreamSource<Student> fileDatasource = env.createInput(inputFormat,pojoType);

        fileDatasource.print();*/


        //2.socket数据源
       /* DataStreamSource<String> socketDatasource = env.socketTextStream("127.0.0.1", 9000);
        socketDatasource.print();*/

        //3.集合
      /*  DataStreamSource<Tuple2> collectionDatasource = env.fromElements(new Tuple2(1, "a"), new Tuple2(2, "b"));
        collectionDatasource.print();*/

        //外部连接器
        //kafka connector
       /* Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers","127.0.0.1:9092");
        consumerConfig.setProperty("group.id", "flinkbase");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer("test", new SimpleStringSchema(), consumerConfig);
        //设置从最早的消息消费
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> kafkaDatasource = env.addSource(kafkaConsumer);
        kafkaDatasource.print();*/

       //自定义数据源
       /* DataStreamSource<String> customSingleDatasource = env.addSource(new CustomSingleSource());
        customSingleDatasource.print();*/

        DataStreamSource<String> cuntomParallelDataspurce = env.addSource(new CustomParallelSource());
        cuntomParallelDataspurce.print();

        env.execute(SourceUse.class.getSimpleName());
    }
}
