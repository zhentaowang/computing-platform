package com.adatafun.computing.platform.dataSource;

import com.adatafun.computing.platform.es.ElasticSearchProcessor;
import com.adatafun.computing.platform.indexMap.PlatformUser;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

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
