package com.adatafun.computing.platform.kafka;

import com.adatafun.computing.platform.es.ElasticSearchProcessor;
import com.adatafun.computing.platform.indexMap.UserMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * ElasticSearchSinkFromKafka.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/11/23.
 */
public class ElasticSearchSinkFromKafka extends RichSinkFunction<Tuple2<String, Long>> {

    private String indexName;
    private String indexType;

    public ElasticSearchSinkFromKafka(String indexName, String indexType) {

        this.indexName = indexName;
        this.indexType = indexType;

    }

    @Override
    public void invoke(Tuple2<String, Long> input) throws Exception {

        Map<String, Object> param = new HashMap<>();
        param.put("indexName", indexName);
        param.put("typeName", indexType);
        param.put("indexId", input.f0);

        ElasticSearchProcessor elasticSearchProcessor = new ElasticSearchProcessor();
        elasticSearchProcessor.setUp();
        UserMap indexMap = new UserMap();
        indexMap.setIP(input.f0);
        indexMap.setInfo(input.f1.toString());
        elasticSearchProcessor.insertOrUpdateDoc(param, indexMap);
        elasticSearchProcessor.tearDown();

    }

}
