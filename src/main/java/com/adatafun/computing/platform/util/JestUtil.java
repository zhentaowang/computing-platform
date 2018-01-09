package com.adatafun.computing.platform.util;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;

/**
 * JestUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 05/09/2017.
 */
public class JestUtil {

    /**
     * 插入或更新文档
     * @param indexId 索引id
     * @param indexObject 索引文档
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @return boolean
     */
    public boolean insertOrUpdateDoc(JestClient jestClient, String indexId, Object indexObject, String indexName, String indexType) {
        Index.Builder builder = new Index.Builder(indexObject);
        builder.id(indexId);
        builder.refresh(true);
        Index index = builder.index(indexName).type(indexType).build();
        try {
            JestResult result = jestClient.execute(index);
            if (result != null && !result.isSucceeded()) {
                throw new RuntimeException(result.getErrorMessage()+"插入更新索引失败!");
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

}
