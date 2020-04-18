package com.paulo.flinkbase.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.client
 * @date:2020/4/18
 */
public class QueryableState {
    public static void main(String[] args) throws IOException {
        //从后台可以看到任务的id
        JobID jobID = JobID.fromHexString("c18ed3040e59635b3ad7069d53a8f41c");
        String key = "odd";

        //flink-core-1.7.0-sources.jar!/org/apache/flink/configuration/QueryableStateOptions.java
        QueryableStateClient client = new QueryableStateClient("127.0.0.1",9069);

        ValueStateDescriptor<Tuple2<String, Double>> valueStateDescriptor = new ValueStateDescriptor<Tuple2<String, Double>>("handle-way", TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
        }));

        CompletableFuture<ValueState<Tuple2<String, Double>>> result = client.getKvState(jobID, "handle-way",key, BasicTypeInfo.STRING_TYPE_INFO, valueStateDescriptor);

        ValueState<Tuple2<String, Double>> valueState = result.join();
        System.out.println(valueState.value());
        client.shutdownAndWait();
    }
}
