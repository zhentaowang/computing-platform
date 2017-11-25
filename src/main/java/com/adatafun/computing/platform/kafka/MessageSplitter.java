package com.adatafun.computing.platform.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * MessageSplitter.java
 * 将获取到的每条Kafka消息根据“，”分割取出其中的机场三字码和用户id
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/11/15.
 */
public class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[1], Long.parseLong(parts[2])));
        }
    }
}
