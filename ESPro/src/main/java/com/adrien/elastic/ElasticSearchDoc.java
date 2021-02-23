package com.adrien.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElasticSearchDoc {

    @Test
    public void searchDocument () throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop103", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("shop");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title","武林"));
        //排序 start
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("_uid").order(SortOrder.ASC));
        //排序 end
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = levelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hitsHits = hits.getHits();
        System.out.println("SearchHit: "+ hitsHits.length);
        for (SearchHit hitsHit : hitsHits) {
            Map<String, Object> sourceAsMap = hitsHit.getSourceAsMap();
            System.out.println(sourceAsMap.toString());
        }
        levelClient.close();
    }

}
