package com.amos.sink;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Title: Sink_Elasticsearch
 * @Date: 2021/6/28 19:02
 */
public class Sink_Elasticsearch {
    public static void main(String[] args) throws Exception {
        StartUpClass startUpClass = new StartUpClass();
        StreamExecutionEnvironment env = startUpClass.getEnv();


        DataStream<String> source = env.addSource(new FlinkKafkaConsumer<String>(
                startUpClass.getTopic(),
                new SimpleStringSchema(),
                startUpClass.getProperties()));

        DataStream<CarSpeedInfo> streamOperator = source.map(line -> {
            String[] strings = line.split("\t");
            return new CarSpeedInfo(Long.parseLong(strings[0]), strings[1], strings[2], Integer.parseInt(strings[3]));
        });

        //定义es 的连接配置
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        streamOperator.addSink(new ElasticsearchSink.Builder<CarSpeedInfo>(
                httpHosts,
                new MyEsSinkFunction()).build());


        env.execute();
    }

    //实现自定义的ES写入操作
    private static class MyEsSinkFunction implements ElasticsearchSinkFunction<CarSpeedInfo> {
        @Override
        public void process(CarSpeedInfo carSpeedInfo, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            //定义写入的数据 source
            Map<String, String> dataSource = new HashMap<>();

            dataSource.put("id", carSpeedInfo.getCarId());
            dataSource.put("speed", carSpeedInfo.getSpeed() + "");
            dataSource.put("ts",carSpeedInfo.getTimeStamp());


            //创建一个request，作为向es发起的写入命令
            IndexRequest source = Requests.indexRequest()
                    .index("sensor")
                    .type("readingsource")
                    .source(dataSource);

            //用index发送请求
            requestIndexer.add(source);

        }
    }
}
