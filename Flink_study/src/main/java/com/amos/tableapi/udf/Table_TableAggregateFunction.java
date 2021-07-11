package com.amos.tableapi.udf;

import com.amos.bean.SensorReading;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: Flink_study
 * @description: 用户自定义函数（UDAF）多对多:用户定义函数必须先注册，然后才能在查询中使用
 * 函数通过registerFunction()方法在TableEnvironment中注册，当用户定义的函数被注册时，
 * 它被插入到TableEnvironment的函数目录中，这样Table API 或 SQL解析器就可以识别并正确地解释它
 * <p>
 * 1.Table Aggregate function （表聚合函数）：可以把一个表中数据聚合为具有多行和多列的结果表
 * @create: 2021-07-10 19:30
 */
public class Table_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //创建Table环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取数据
        DataStream<String> stringDataStreamSource =
                env.readTextFile("D:\\FLINK\\Flink-study\\Flink_study\\src\\main\\resources\\sensor.txt");

        //转换成pojo
        DataStream<SensorReading> dataStream = stringDataStreamSource.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimeStamp() * 1000L;
                    }
                });

        //将流转换成表
        Table dataTable = tableEnv
                .fromDataStream(dataStream, "id,timeStamp as ts,temperature as temp,rt.rowtime");
        dataTable.printSchema();

        //自定义实现聚合函数：求当前传感器的平均温度
        Top2Temp top2Temp = new Top2Temp();

        //需要在环境中进行注册udf
        tableEnv.registerFunction("top2Temp", top2Temp);

        Table resultTable = dataTable.groupBy("id")
                .flatAggregate("top2Temp(temp) as top2temp")
                .select("id,top2temp");


        tableEnv.toRetractStream(resultTable, Row.class).print("result");


        env.execute();
    }

    //实现自定义的AggregateFunction
    public static class Top2Temp extends TableAggregateFunction<Double, List<Double>> {


        @Override
        public List<Double> createAccumulator() {
            List arrayList = new ArrayList();
            arrayList.add(0.0);
            return arrayList;
        }

        //更新累加器
        public void accumulate(List<Double> tempList, Double temp) {

            for (int i = 0; i < tempList.size(); i++) {
                if (temp > tempList.get(i))
                    tempList.set(i, temp);
            }
        }

        public void emitValue(List<Double> accumulator, Collector<Double> out) {

            for (int i = 0; i < accumulator.size(); i++) {

                out.collect(accumulator.get(i));
            }
        }


    }

}
