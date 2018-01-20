package com.adatafun.computing.platform.es;

import com.adatafun.computing.platform.conf.ESFactory;
import com.adatafun.computing.platform.model.PlatformUser;
import com.adatafun.computing.platform.util.JestUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.*;

/**
 * ElasticSearchProcessor.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 05/09/2017.
 */
public class ElasticSearchProcessor {

    private JestClient jestClient;
    private ESFactory esFactory;
    private JestUtil jestUtil;

    @Before
    public void setUp() throws IOException{

        esFactory = new ESFactory();
        jestUtil = new JestUtil();
        jestClient = esFactory.getJestClient();

    }

    @After
    public void tearDown() {

        esFactory.closeJestClient(jestClient);

    }

    private boolean partUpdateDoc(Map<String, Object> param, Object indexObject) {

        return jestUtil.partUpdateDoc(jestClient, param.get("indexId").toString(),
                indexObject, param.get("indexName").toString(), param.get("typeName").toString());

    }

    public boolean insertOrUpdateDoc(Map<String, Object> param, Object indexObject) {

        return jestUtil.insertOrUpdateDoc(jestClient, param.get("indexId").toString(),
                indexObject, param.get("indexName").toString(), param.get("typeName").toString());

    }

    private boolean deleteDoc(Map<String, Object> param) {

        return jestUtil.deleteDoc(jestClient, param.get("indexId").toString(),
                param.get("indexName").toString(), param.get("typeName").toString());

    }

    private PlatformUser getPlatformUser(Map<String, Object> param) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.termQuery("longTengId", param.get("longTengId")))
                        .should(QueryBuilders.termQuery("baiYunId", param.get("baiYunId"))));
        searchSourceBuilder.query(queryBuilder);
        String query = searchSourceBuilder.toString();
        SearchResult result = jestUtil.searchDoc(jestClient, param.get("indexName").toString(), param.get("typeName").toString(), query);
        List<SearchResult.Hit<PlatformUser, Void>> hits = result.getHits(PlatformUser.class);
        PlatformUser user = new PlatformUser();
        if (hits != null && hits.size() != 0) {
            user = hits.get(0).source; //在es中用户是唯一的
        }
        return user;

    }

    public void writeToESForMerge(PlatformUser input1, PlatformUser input2,String indexName, String indexType) throws IOException {

        Map<String, Object> param = new HashMap<>();
        param.put("indexName", indexName);
        param.put("typeName", indexType);
        param.put("longTengId", input1.getLongTengId());
        param.put("baiYunId", input2.getBaiYunId());

        setUp();
        PlatformUser platformUser = getPlatformUser(param);
        tearDown();

        String longTengId = platformUser.getLongTengId();
        String baiYunId = platformUser.getBaiYunId();
//        if (longTengId.equals(input1.getLongTengId()) || baiYunId.equals(input2.getBaiYunId())) {
//            if (longTengId.equals(input1.getLongTengId())) {
//
//            }
//        }

        param.put("indexId", input1.getLongTengId() + '*' + input2.getBaiYunId());
        setUp();
        insertOrUpdateDoc(param, input1);
        tearDown();

    }

    public void writeToESByPartUpdate(Map input, String indexName, String indexType) throws IOException {

        Map<String, Object> param = new HashMap<>();
        param.put("indexName", indexName);
        param.put("typeName", indexType);

        param.put("indexId", input.get("longTengId").toString());
        setUp();
        partUpdateDoc(param, input);
        tearDown();

    }

    public void writeToESByDay(PlatformUser input, String indexName, String indexType) throws IOException {

        Map<String, Object> param = new HashMap<>();
        param.put("indexName", indexName);
        param.put("typeName", indexType);

        param.put("indexId", input.getLongTengId() + '*' + input.getBaiYunId());
        setUp();
        insertOrUpdateDoc(param, input);
        tearDown();

    }

    public void writeToES(PlatformUser input, String indexName, String indexType) throws IOException {

        Map<String, Object> param = new HashMap<>();
        param.put("indexName", indexName);
        param.put("typeName", indexType);

        if (!input.getLongTengId().equals("") && !input.getBaiYunId().equals("")) {

            param.put("indexId", input.getLongTengId() + '*');
            setUp();
            deleteDoc(param);
            tearDown();

            param.put("indexId", '*' + input.getBaiYunId());
            setUp();
            deleteDoc(param);
            tearDown();

            param.put("indexId", input.getLongTengId() + '*' + input.getBaiYunId());
            setUp();
            insertOrUpdateDoc(param, input);
            tearDown();

        } else {

            param.put("indexId", input.getLongTengId() + '*' + input.getBaiYunId());
            setUp();
            insertOrUpdateDoc(param, input);
            tearDown();

        }

    }

}