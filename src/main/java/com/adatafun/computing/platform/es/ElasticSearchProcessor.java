package com.adatafun.computing.platform.es;

import com.adatafun.computing.platform.conf.ESFactory;
import com.adatafun.computing.platform.model.PlatformUser;
import com.adatafun.computing.platform.util.JestUtil;
import io.searchbox.client.JestClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

    public boolean insertOrUpdateDoc(Map<String, Object> param, Object indexObject) {

        return jestUtil.insertOrUpdateDoc(jestClient, param.get("indexId").toString(),
                indexObject, param.get("indexName").toString(), param.get("typeName").toString());

    }

    public void writeToES(PlatformUser input, String indexName, String indexType) throws IOException {

        Map<String, Object> param = new HashMap<>();
        param.put("indexName", indexName);
        param.put("typeName", indexType);
        param.put("indexId", input.getLongTengId());

        setUp();
        insertOrUpdateDoc(param, input);
        tearDown();

    }

}