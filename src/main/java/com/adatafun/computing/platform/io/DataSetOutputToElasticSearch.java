package com.adatafun.computing.platform.io;

import com.adatafun.computing.platform.es.ElasticSearchProcessor;
import com.adatafun.computing.platform.model.PlatformUser;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

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

        ElasticSearchProcessor elasticSearchProcessor = new ElasticSearchProcessor();
        elasticSearchProcessor.writeToES(input, indexName, indexType);

    }

    @Override
    public void close() throws IOException {

    }

}
