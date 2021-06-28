package com.amos.sink;

import com.amos.bean.CarSpeedInfo;
import com.amos.util.StartUpClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Title: Sink_Mysql
 * @Date: 2021/6/28 19:42
 */
public class Sink_Mysql {
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
        streamOperator.addSink(new MyJDBCSinkFunction());

        env.execute();
    }

    private static class MyJDBCSinkFunction extends RichSinkFunction<CarSpeedInfo> {

        private Connection connection = null;
        //声明一个预编译语句
        private PreparedStatement insertStmt = null;
        private PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            //预编译语句
            insertStmt = connection.prepareStatement("insert into senser_temp(id,speed) values(?,?)");
            updateStmt = connection.prepareStatement("update   senser_temp set speed=? where id=?");
        }
        //每来一条数据，调用连接，执行sql
        @Override
        public void invoke(CarSpeedInfo value, Context context) throws Exception {

            //直接执行更新语句，如果没有更新那么就插入
            updateStmt.setDouble(1,value.getSpeed());
            updateStmt.setString(2,value.getCarId());
            updateStmt.execute();

            if(updateStmt.getUpdateCount()==0){
                insertStmt.setString(1,value.getCarId());
                insertStmt.setDouble(2,value.getSpeed());
                insertStmt.execute();
            }

        }

        @Override
        public void close() throws Exception {
            insertStmt.close();;
            updateStmt.close();
            connection.close();
        }
    }
}
