package com.paulo.flinkbase.app;

import com.paulo.flinkbase.model.ItemViewCount;
import com.paulo.flinkbase.model.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;

/**
 * TopN热门商品程序
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取资源路径
        URL resource = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(resource.toURI()));

        //获取转换的flink的pojo类型
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldNames = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat inputFormat = new PojoCsvInputFormat(filePath, pojoType, fieldNames);
        //数据源
        DataStreamSource datasource = env.createInput(inputFormat, pojoType);

        //eventTime作为处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //因为数据默认都是有序的所以用AscendingTimestampExtractor现实中肯能是无序的可以用BoundedOutOfOrdernessTimestampExtractor
        DataStream<UserBehavior> pvData = datasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp();
            }
        }).filter(new FilterFunction<UserBehavior>() {

            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        });

        DataStream<ItemViewCount> viewCount = pvData.map(new MapFunction<UserBehavior, ItemViewCount>() {
            public ItemViewCount map(UserBehavior userBehavior) throws Exception {
                ItemViewCount itemViewCount = new ItemViewCount();
                itemViewCount.setItemId(userBehavior.getItemId());
                itemViewCount.setViewCount(1);
                return itemViewCount;
            }
        });


        //统计每个小时内每种商品的浏览量
        DataStream<ItemViewCount> windowData = viewCount
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .sum("viewCount");


        //找出topN的浏览量
        SingleOutputStreamOperator<ArrayList<ItemViewCount>> topNData = windowData
                //欺骗分类器将所有数据都放到一个key去处理
                .keyBy(new KeySelector<ItemViewCount, String>() {
                    @Override
                    public String getKey(ItemViewCount itemViewCount) throws Exception {
                        return "";
                    }
                })
                .fold(new ArrayList<ItemViewCount>(), new FoldFunction<ItemViewCount, ArrayList<ItemViewCount>>() {
                    public ArrayList<ItemViewCount> fold(ArrayList<ItemViewCount> itemViewCounts, ItemViewCount itemViewCount) {
                        if (itemViewCounts.size() < 3) {
                            itemViewCounts.add(itemViewCount);
                            itemViewCounts.sort((o1, o2) -> (int) (o2.getViewCount() - o1.getViewCount()));
                        } else {
                            ItemViewCount lastItem = itemViewCounts.get(itemViewCounts.size() - 1);
                            if (lastItem.getViewCount() < itemViewCount.getViewCount()) {
                                itemViewCounts.remove(itemViewCounts.size() - 1);
                                itemViewCounts.add(itemViewCount);
                                itemViewCounts.sort((o1, o2) -> (int) (o2.getViewCount() - o1.getViewCount()));
                            }
                        }
                        return itemViewCounts;
                    }
                });

        topNData.print();

        env.execute(HotItems.class.getSimpleName());
    }
}
