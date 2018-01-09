package com.adatafun.computing.platform.io;

import com.adatafun.computing.platform.es.ElasticSearchProcessor;
import com.adatafun.computing.platform.model.PlatformUser;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * DataStreamOutputToElasticSearch.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/6.
 */
public class DataStreamOutputToElasticSearch extends RichSinkFunction<PlatformUser> {

    private String indexName;
    private String indexType;

    public DataStreamOutputToElasticSearch(String indexName, String indexType) {

        this.indexName = indexName;
        this.indexType = indexType;

    }

    @Override
    public void invoke(PlatformUser input) throws Exception {

        ElasticSearchProcessor elasticSearchProcessor = new ElasticSearchProcessor();
        elasticSearchProcessor.writeToES(input, indexName, indexType);

    }

}
