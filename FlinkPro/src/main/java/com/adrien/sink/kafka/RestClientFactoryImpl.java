package com.adrien.sink.kafka;

import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClientBuilder;


public class RestClientFactoryImpl implements RestClientFactory {
    @Override
    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
        Header[] headers =new BasicHeader[]{new BasicHeader("Content-Type","application/json")};
        restClientBuilder.setDefaultHeaders(headers);
        restClientBuilder.setMaxRetryTimeoutMillis(90000);
    }
}
