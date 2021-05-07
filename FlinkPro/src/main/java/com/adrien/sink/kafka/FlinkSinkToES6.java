package com.adrien.sink.kafka;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class FlinkSinkToES6 {
//    private static final Logger log = LoggerFactory.getLogger(FlinkSinkToES6.class);
//
//    private static final String READ_TOPIC = "student-1";
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("zookeeper.connect", "localhost:2181");
//        props.put("group.id", "student-group-1");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "latest");
//        DataStreamSource kafkaStream = enev.addSource(new FlinkKafkaConsumer011(
//                //这个 kafka topic 需要和上面的工具类的 topic 一致
//                READ_TOPIC,
//                new SimpleStringSchema(),
//                props
//        )).setParallelism(1);
//        kafkaStream.print();
//        log.info("student:" + kafkaStream);
//
//        ArrayList<HttpHost> esHttpHost = new ArrayList<HttpHost>();
//        esHttpHost.add(new HttpHost("127.0.0.1", 9200, "http"));
//        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
//                esHttpHost,
//                new ElasticsearchSinkFunction<String>() {
//
//                    public IndexRequest createIndexRequest(String element) {
//                        HashMap<Object, Object> jsons = new HashMap<>();
//                        jsons.put("data", element);
//                        log.info("data:" + element);
//                        return Requests.indexRequest()
//                                .index("index-student")
//                                .type("student")
//                                .source(jsons);
//                    }
//
//                    @Override
//                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                        indexer.add(createIndexRequest(element));
//                    }
//                }
//        );
//        esSinkBuilder.setBulkFlushMaxActions(1);
//        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
//        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
//        kafkaStream.addSink(esSinkBuilder.build());
//        enev.execute("Flink Learning Connectors Kafka to ES");
//    }
}
