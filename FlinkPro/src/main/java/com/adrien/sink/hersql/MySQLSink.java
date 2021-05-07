package com.adrien.sink.hersql;

import com.adrien.sources.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.streaming.connectors.redis.RedisSink;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * 完整的代码如下，实现一个读取Kafka的消息，然后进行WordCount，并把结果更新到redis中：
 */
public class MySQLSink extends RichSinkFunction<Tuple3<String,String,String>> {

    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement statement;

    // JDBC连接信息
    @Override
    public void open(Configuration parameters) throws Exception {
        String USERNAME = "root" ;
        String PASSWORD = "123123";
        String DRIVERNAME = "com.mysql.jdbc.Driver";
        String DBURL = "jdbc:mysql://hadoop101:3306/flink";
        // 加载JDBC驱动
        Class.forName(DRIVERNAME);
        connection = DriverManager.getConnection(DBURL, USERNAME, PASSWORD);
        String sql = "insert into kafka_message(id, age, name,password) values (?,14,?,?)";
        statement = connection.prepareStatement(sql);
        super.open(parameters);
    }

    /**
     * invoke()方法解析一个元组数据，并插入到数据库中。
     * @param data 输入的数据
     * @throws Exception
     */
    @Override
    public void invoke(Tuple3<String,String,String> data) throws Exception {
        try {
            String id = data.getField(0);
            String name = data.getField(1);
            String password = data.getField(2);
            statement.setString(1,id);
            statement.setString(2,name);
            statement.setString(3,password);
            statement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if(statement != null){
            statement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
}


