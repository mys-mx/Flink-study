package com.amos.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Title:
 * @Description: 流处理
 * @Author: YuSong.Mu
 * @Date: 2021/6/20 17:02
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {

        //流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop01", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String words, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = words.split(" ");
                for (String word : strings) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        flatMap.keyBy(0).sum(1).print();


        env.execute();

    }


}
