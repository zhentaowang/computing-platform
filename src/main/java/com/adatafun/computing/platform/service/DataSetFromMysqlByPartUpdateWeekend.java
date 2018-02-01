package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByPartUpdate;
import com.adatafun.computing.platform.util.DateUtil;
import com.adatafun.computing.platform.util.LabelProcessingUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * DataSetFromMysqlByPartUpdateWeekend.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/18.
 */
public class DataSetFromMysqlByPartUpdateWeekend {

    public static void main(String[] args) throws Exception {

        DateUtil holidayUtil1 = new DateUtil();
        Map<String, String> map = holidayUtil1.getDateInterval(Calendar.DAY_OF_MONTH, -90);//把日期往前减90天
        String startTime = map.get("startTime");
        String stopTime = map.get("stopTime");

        MysqlConf mysqlConf = new MysqlConf();
        DataSetInputFromMysqlParam dataSetInput = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl3(),
                mysqlConf.getUsername3(), mysqlConf.getPassword3(), mysqlConf.getSql3(), startTime, stopTime);
        List<Map> userList = dataSetInput.readFromMysqlByPartUpdate();
        System.out.println(userList.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Integer split = 100000;
        DataSet<Tuple3<String, String, Integer>> dataUnion = null;
        Integer count = userList.size();
        for (int i = 0; i < count/split+1; i++) {
            Integer toIndex = count > (i+1)*split ? (i+1)*split : count;
            DataSet<Map> input_user = env.fromCollection(userList.subList(i*split, toIndex));
            DataSet<Tuple3<String, String, Integer>> input = input_user.flatMap(new FlatMapFunction<Map, Tuple3<String, String, Integer>>() {
                public void flatMap(Map value, Collector<Tuple3<String, String, Integer>> out) throws ParseException {

                    Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
                    DateUtil holidayUtil = new DateUtil();

                    String loginDate = value.get("createTime").toString();
                    Date date = holidayUtil.getDate(loginDate);

                    if (holidayUtil.isWeekend(date)) {
                        tuple3.setFields(value.get("longTengId").toString(), "周末出行", 1);
                    } else {
                        tuple3.setFields(value.get("longTengId").toString(), "非周末出行", 1);
                    }

                    out.collect(tuple3);

                }
            }).groupBy(0, 1).sum(2);

            if (i == 0) {
                dataUnion = input;
            } else {
                dataUnion.union(input);
            }

//            input.print();
//            System.out.println(input.count());
        }

        if (dataUnion != null) {

            DataSet<Map> dataCount = dataUnion.groupBy(0,1).sum(2)
                    .flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Map>() {
                        public void flatMap(Tuple3<String, String, Integer> value, Collector<Map> out) throws ParseException{
                            LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                            out.collect(labelProcessingUtil.weekendTravel(value));
                        }
                    });

            dataCount.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
            dataCount.print();
            System.out.println(dataCount.count());
        }

    }

}
