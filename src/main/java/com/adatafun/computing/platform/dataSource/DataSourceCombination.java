package com.adatafun.computing.platform.dataSource;

import com.adatafun.computing.platform.indexMap.UnionUser;
import com.adatafun.computing.platform.kafka.MysqlSink;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * XXX.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/27.
 */
public class DataSourceCombination {
    public static void main(String[] args) throws Exception {

        String driver1 = "com.mysql.jdbc.Driver";
        String url1 = "jdbc:mysql://localhost:3306/demo?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
        String username1 = "root";
        String password1 = "w19890528";
        Class.forName(driver1);
        String sql1 = "SELECT name,address,sex FROM student;";

        String driver2 = "com.mysql.jdbc.Driver";
        String url2 = "jdbc:mysql://localhost:3306/demo00?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
        String username2 = "root";
        String password2 = "w19890528";
        Class.forName(driver2);
        String sql2 = "SELECT name,phone,card FROM user;";

        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        final long windowSize = 2000;
        DataStream<UnionUser> students = env.addSource(new DataSourceInput(driver1, url1, username1, password1, sql1));
        DataStream<UnionUser> users = env.addSource(new DataSourceInput(driver2, url2, username2, password2, sql2));
//        students.union(users).addSink(new MysqlSink());
        DataStream<UnionUser> ss = students.join(users).where(new NameKeySelector()).equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new JoinFunction<UnionUser, UnionUser, UnionUser>() {

                    @Override
                    public UnionUser join(
                            UnionUser first,
                            UnionUser second) {
                        return new UnionUser(first.getName(), first.getSex(), second.getPhone(), second.getCard(), first.getAddress());
                    }
                });
        students.print();
        users.print();
        ss.print();
        ss.addSink(new MysqlSink());

//        students.addSink(new MysqlSink());

        //触发流执行
        env.execute();
    }

    private static class NameKeySelector implements KeySelector<UnionUser, String> {
        @Override
        public String getKey(UnionUser value) {
            return value.getName();
        }
    }
}
