package com.adrien.elastic.test;


import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.io.IOException;

public class ESTest {



    //创建索引
    @Test
    public void createIndex() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        CreateIndexRequest indexReq = new CreateIndexRequest("goods");
        // 创建的每个索引都可以有与之关联的特定设置
        indexReq.settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        indexReq.timeout(TimeValue.timeValueMinutes(2));//超时,等待所有节点被确认(使用TimeValue方式)
        indexReq.timeout("2m");//超时,等待所有节点被确认(使用字符串方式)
        indexReq.masterNodeTimeout(TimeValue.timeValueMinutes(1));// 连接master节点的超时时间(使用TimeValue方式)
        indexReq.masterNodeTimeout("1m");//连接master节点的超时时间(使用字符串方式)
        indexReq.waitForActiveShards(2);//在创建索引API返回响应之前等待的活动分片副本的数量，以int形式表示。
        CreateIndexResponse indexResponse = levelClient.indices().create(indexReq, RequestOptions.DEFAULT);
        levelClient.close();
        System.out.println("isAcknowledged:"
                + indexResponse.isAcknowledged());
        System.out.println("isShardsAcknowledged:"
                + indexResponse.isShardsAcknowledged());
    }

    //检查索引是否存在
    @Test
    public void isExitIndex () throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices("goods");
        boolean exists = levelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println("索引goods:" + exists);
        levelClient.close();
    }
}





















