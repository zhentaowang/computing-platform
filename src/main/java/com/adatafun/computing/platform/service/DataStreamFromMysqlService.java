package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.io.DataStreamInputFromMysql;
import com.adatafun.computing.platform.io.DataStreamOutputToElasticSearch;
import com.adatafun.computing.platform.model.PlatformUser;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * DataStreamFromMysqlService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/27.
 */
public class DataStreamFromMysqlService {

    public static void main(String[] args) throws Exception {

        String driver = "com.mysql.jdbc.Driver";

        String url1 = "jdbc:mysql://localhost:3306/demo?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
        String username1 = "root";
        String password1 = "w19890528";
        String sql1 = "select id as longTengId, mobileNo as phoneNum, cardNo as idNum from student;";

        String url2 = "jdbc:mysql://localhost:3306/demo00?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
        String username2 = "root";
        String password2 = "w19890528";
        String sql2 = "select id as baiYunId, phone as phoneNum, card as idNum from user;";

        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        final long windowSize = 2000;

        JoinFunction<PlatformUser, PlatformUser, PlatformUser> joinFunction = (PlatformUser first, PlatformUser second) ->
         (new PlatformUser(first.getLongTengId(), first.getPhoneNum(), second.getBaiYunId(), second.getIdNum()));

        DataStream<PlatformUser> students = env.addSource(new DataStreamInputFromMysql(driver, url1, username1, password1, sql1));
        DataStream<PlatformUser> users = env.addSource(new DataStreamInputFromMysql(driver, url2, username2, password2, sql2));
        DataStream<PlatformUser> result = students.join(users).where(new NameKeySelector()).equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(joinFunction);
//                .apply(new JoinFunction<PlatformUser, PlatformUser, PlatformUser>() {
//
//                    @Override
//                    public PlatformUser join(
//                            PlatformUser first,
//                            PlatformUser second) {
//                        return new PlatformUser(first.getLongTengId(), first.getPhoneNum(), second.getBaiYunId(), second.getIdNum());
//                    }
//                });
        result.print();
//        result.addSink(new MysqlSink());
        result.addSink(new DataStreamOutputToElasticSearch("dmp-user", "dmp-user"));

        //触发流执行
        env.execute();
    }

    private static class NameKeySelector implements KeySelector<PlatformUser, String> {
        @Override
        public String getKey(PlatformUser value) {
            return value.getPhoneNum();
        }
    }

}
