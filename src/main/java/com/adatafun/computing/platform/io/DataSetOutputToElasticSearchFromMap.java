package com.adatafun.computing.platform.io;

import com.adatafun.computing.platform.es.ElasticSearchProcessor;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * DataSetOutputToElasticSearchFromMap.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/21.
 */
public class DataSetOutputToElasticSearchFromMap implements OutputFormat<Map<String, Object>> {

    private String indexName;
    private String indexType;

    public DataSetOutputToElasticSearchFromMap(String indexName, String indexType) {

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
    public void writeRecord(Map<String, Object> input) throws IOException {

        ElasticSearchProcessor elasticSearchProcessor = new ElasticSearchProcessor();
        elasticSearchProcessor.writeToESFromMap(input, indexName, indexType);

    }

    @Override
    public void close() throws IOException {

    }

}
