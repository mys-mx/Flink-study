package com.amos.tableapi.udf;

import com.amos.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @program: Flink_study
 * @description: 用户自定义函数（UDAF）多对一:用户定义函数必须先注册，然后才能在查询中使用
 * 函数通过registerFunction()方法在TableEnvironment中注册，当用户定义的函数被注册时，
 * 它被插入到TableEnvironment的函数目录中，这样Table API 或 SQL解析器就可以识别并正确地解释它
 * <p>
 * 1.Aggregate function （聚合函数）：可以把一个表的数据聚合成一个标量值
 * @create: 2021-07-10 19:30
 */
public class Table_AggregateFunction {
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
        AvgTemp avgTemp = new AvgTemp();

        //需要在环境中进行注册udf
        tableEnv.registerFunction("avgTemp", avgTemp);

        Table resultTable = dataTable.groupBy("id")
                .aggregate("avgTemp(temp) as avgtemp")
                .select("id,avgtemp");

        //SQL 先注册一张表
        tableEnv.createTemporaryView("sensor", dataTable);
        Table sqlQuery = tableEnv.sqlQuery("select id,avgTemp(temp) avgtemp " +
                "from sensor group by id");

        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(sqlQuery, Row.class).print("SQL");


        env.execute();
    }

    //实现自定义的AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {


        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        //必须实现一个accumulate方法,来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> acc, Double temp) {
            acc.f0 += temp;
            acc.f1 += 1;
        }

    }

}
