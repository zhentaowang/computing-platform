package com.adatafun.computing.platform.util;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;

import java.util.List;
import java.util.Map;

/**
 * JestUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 05/09/2017.
 */
public class JestUtil {

    /**
     * 插入或更新文档
     * @param indexId 索引id
     * @param indexObjects 索引文档
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @return boolean
     */
    public boolean bulkInsertOrUpdateDoc(JestClient jestClient, String indexId, List<Map> indexObjects, String indexName, String indexType) {
        Bulk.Builder bulkBuilder = new Bulk.Builder();
        for (Map map : indexObjects) {
            Index index = new Index.Builder(map).index(indexName).type(indexType).id(indexId).build();
            bulkBuilder.addAction(index);
        }
        try {
            JestResult result = jestClient.execute(bulkBuilder.build());
            if (result != null && !result.isSucceeded()) {
                throw new RuntimeException(result.getErrorMessage()+"批量插入或更新索引文档失败!");
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 部分更新文档
     * @param indexId 索引id
     * @param indexObject 索引文档
     * @param indexName 索引名称
     * @param indexType 索引类型
     * @return boolean
     */
    public boolean partUpdateDoc(JestClient jestClient, String indexId, Object indexObject, String indexName, String indexType) {

        PartUpdateToESUtil.Builder builder = new PartUpdateToESUtil.Builder(indexObject);
        builder.id(indexId);
        builder.refresh(true);
        PartUpdateToESUtil update = builder.index(indexName).type(indexType).build();
        try {
            JestResult result = jestClient.execute(update);
            if (result != null && !result.isSucceeded()) {
                throw new RuntimeException(result.getErrorMessage()+"部分更新索引文档失败!");
            }
            return true;
        } catch (Exception e) {
            return false;
        }

    }

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
                throw new RuntimeException(result.getErrorMessage()+"插入更新索引文档失败!");
            }
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    /**
     * Delete文档
     * @return boolean
     */
    public boolean deleteDoc(JestClient jestClient, String indexId, String indexName, String typeName) {

        try {
            JestResult result = jestClient.execute(new Delete.Builder(indexId).index(indexName).type(typeName).build());
            if (result != null && !result.isSucceeded()) {
                throw new RuntimeException(result.getErrorMessage()+"删除索引文档失败!");
            }
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    /**
     * 搜索文档
     * @return SearchResult
     */
    public SearchResult searchDoc(JestClient jestClient, String indexName, String typeName, String query) {

        try {
            Search search = new Search.Builder(query)
                    .addIndex(indexName)
                    .addType(typeName)
                    .build();
            SearchResult result = jestClient.execute(search);
            if (result != null && !result.isSucceeded()) {
                throw new RuntimeException(result.getErrorMessage()+"查询文档失败!");
            }
            return result;
        } catch (Exception e) {
            return null;
        }

    }

}
