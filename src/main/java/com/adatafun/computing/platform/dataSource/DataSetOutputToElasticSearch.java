package com.adatafun.computing.platform.dataSource;

import com.adatafun.computing.platform.es.ElasticSearchProcessor;
import com.adatafun.computing.platform.indexMap.PlatformUser;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DataSetOutputToElasticSearch.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/6.
 */
public class DataSetOutputToElasticSearch implements OutputFormat<PlatformUser> {

    private String indexName;
    private String indexType;

    public DataSetOutputToElasticSearch(String indexName, String indexType) {

        this.indexName = indexName;
        this.indexType = indexType;

    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {

    }

    @Override
    public void writeRecord(PlatformUser input) throws IOException {

        Map<String, Object> param = new HashMap<>();
        param.put("indexName", indexName);
        param.put("typeName", indexType);
        param.put("indexId", input.getBaiYunId());

        ElasticSearchProcessor elasticSearchProcessor = new ElasticSearchProcessor();
        elasticSearchProcessor.setUp();
        elasticSearchProcessor.insertOrUpdateDoc(param, input);
        elasticSearchProcessor.tearDown();

    }

    @Override
    public void close() throws IOException {

    }

}
