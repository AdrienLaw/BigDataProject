package com.adrien.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticDocumentClient {

    //创建更新文档
    @Test
    public void insertDocument () throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("title","一个人的武林");
        jsonMap.put("create_date","2021-02-23");
        IndexRequest indexRequest = new IndexRequest("shop", "goods", "6").source(jsonMap);
        IndexResponse indexResponse = levelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("ID  " + indexResponse.getId());
        System.out.println("ID  " + indexResponse.getIndex());
        levelClient.close();
    }

    //获取文档
    @Test
    public void getDocument () throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        GetRequest getRequest = new GetRequest("shop", "goods", "2");
        GetResponse getResponse = levelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("数据如下：" + getResponse.getSource().toString());
        levelClient.close();
    }
    /**
     * Response
     *
     * 数据如下：{title=放羊的星星, create_date=2021-02-23}
     */

    //检查文档是否存在
    @Test
    public void isExistDocument () throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        GetRequest getRequest = new GetRequest("shop", "goods", "2");
        boolean exists = levelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("Document is exits " + exists);
        levelClient.close();
    }

    //删除文档
    @Test
    public void deleteDocument () throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        DeleteRequest deleteRequest = new DeleteRequest("shop", "goods", "2");
        deleteRequest.timeout(TimeValue.timeValueMinutes(2));
        DeleteResponse deleteResponse = levelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getSeqNo());
        levelClient.close();
    }

    //更新文档
    @Test
    public void updateDocument() throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        Map<Object, Object> jsonMap = new HashMap<>();
        jsonMap.put("title", "公主小妹");
        jsonMap.put("create_date", "2021-02-24");
        UpdateRequest updateRequest = new UpdateRequest("shop", "goods", "2").doc(jsonMap);
        UpdateResponse updateResponse = levelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.getId());
        levelClient.close();
    }



}
