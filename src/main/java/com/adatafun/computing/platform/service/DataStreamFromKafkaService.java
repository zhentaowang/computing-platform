package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.es.ElasticSearchProcessor;
import com.adatafun.computing.platform.kafka.ElasticSearchSinkFromKafka;
import com.adatafun.computing.platform.kafka.MessageSplitter;
import com.adatafun.computing.platform.kafka.MessageWaterEmitter;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * DataStreamFromKafkaService.java
 * Flink入口类，封装了对于Kafka消息的处理逻辑。本例每100毫秒统计一次结果并写入到es
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/11/25.
 */
@Service
public class DataStreamFromKafkaService {

    public String messageStreamingComputing(final JSONObject request) {

        try {
            Properties props = new Properties();
            InputStream in = new BufferedInputStream(new FileInputStream("src/main/resources/application.properties"));
            props.load(in);     ///加载属性列表
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(Integer.valueOf(props.getProperty("flink.checkPointing"))); // 非常关键，一定要设置启动检查点！！
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            Properties kafkaProp = new Properties();
            kafkaProp.put("bootstrap.servers", props.getProperty("kafka.bootstrap.servers"));
            kafkaProp.put("group.id", props.getProperty("kafka.group.id"));

            FlinkKafkaConsumer010<String> consumer =
                    new FlinkKafkaConsumer010<>(request.getString("topic"), new SimpleStringSchema(), kafkaProp);
            consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

            DataStream<Tuple2<String, Long>> keyedStream = env
                    .addSource(consumer)
                    .flatMap(new MessageSplitter())
                    .keyBy(0)
                    .timeWindow(Time.milliseconds(100))
                    .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
                        @Override
                        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                            long sum = 0L;
                            int count = 0;
                            for (Tuple2<String, Long> record: input) {
                                sum += record.f1;
                                count++;
                            }
                            Tuple2<String, Long> result = input.iterator().next();
                            result.f1 = sum / count;
                            out.collect(result);
                        }
                    });

            keyedStream.print();
            keyedStream.addSink(new ElasticSearchSinkFromKafka(request.getString("indexName"), request.getString("indexType")));
//          keyedStream.writeAsText("C:\\Users\\wzt\\Desktop\\flinkResult");
            env.execute("Flink-Kafka demo");
            return "成功";
        } catch (Exception e) {
            return "失败";
        }

    }

    public String getUserLabel(final JSONObject request) {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> resultMap = new HashMap<>();
        try {
            String longTengId = request.getString("longTengId") + '*';
            String indexName = "dmp-user";
            String indexType = "dmp-user";
            List<Map> labelList = JSON.parseArray(request.getString("labelList"), Map.class);
            ElasticSearchProcessor elasticSearchProcessor = new ElasticSearchProcessor();
            Map map = elasticSearchProcessor.getUserLabel(indexName, indexType, longTengId);
            List<String> resultLabel = new ArrayList<>();
            for (Map label : labelList) {
                String labelName = label.get("labelName").toString();
                String labelValue_match = label.get("labelValue").toString();
                if (map.containsKey(labelName)) {
                    String labelValue_user = map.get(labelName).toString();
                    String[] labelValue_array = labelValue_user.split(",");
                    for (String value : labelValue_array) {
                        if (labelValue_match.contains(value)) {
                            resultLabel.add(value);
                        }
                    }
                }
            }
            resultMap.put("labelList", resultLabel);
            result.put("data", resultMap);
            result.put("state", "200");
            result.put("message", "操作成功");
            return JSON.toJSONString(result);
        } catch (Exception e) {
            result.put("data", null);
            result.put("state", "500");
            result.put("message", e.getMessage());
            return JSON.toJSONString(result);
        }
    }

}
