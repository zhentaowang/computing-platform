package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByPartUpdate;
import com.adatafun.computing.platform.util.DateUtil;
import com.adatafun.computing.platform.util.LabelProcessingUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateHoliday.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/19.
 */
public class DataSetFromMysqlByPartUpdateHoliday {

    public static void main(String[] args) throws Exception {

        DateUtil dateUtil = new DateUtil();
        Map<String, String> map = dateUtil.getDateInterval(Calendar.YEAR, -1);//把日期往前减1年
        String startTime_holiday = map.get("startTime");
        String stopTime_holiday = map.get("stopTime");

        MysqlConf mysqlConf = new MysqlConf();
        DataSetInputFromMysqlParam dataSetInput = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl3(),
                mysqlConf.getUsername3(), mysqlConf.getPassword3(), mysqlConf.getSql3(), startTime_holiday, stopTime_holiday);
        List<Map> userList = dataSetInput.readFromMysqlByPartUpdate();
        System.out.println(userList.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Integer split = 100000;
        DataSet<Tuple2<String, String>> dataUnion = null;
        Integer count = userList.size();
        for (int i = 0; i < count/split+1; i++) {
            Integer toIndex = count > (i+1)*split ? (i+1)*split : count;
            DataSet<Map> input_user = env.fromCollection(userList.subList(i*split, toIndex));
            DataSet<Tuple2<String, String>> input = input_user.flatMap(new FlatMapFunction<Map, Tuple2<String, String>>() {
                public void flatMap(Map value, Collector<Tuple2<String, String>> out) throws ParseException{
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    out.collect(labelProcessingUtil.holidayTravel(value));
                }
            });

            if (i == 0) {
                dataUnion = input;
            } else {
                dataUnion.union(input);
            }

//            input.print();
//            System.out.println(input.count());
        }

        if (dataUnion != null) {
            DataSet<Map> dataTransfer = dataUnion.groupBy(0).sortGroup(1, Order.ASCENDING).first(1)
                    .flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
                        public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                            DateUtil dateUtil1 = new DateUtil();
                            Map<String, Object> map = new HashMap<>();
                            map.put("longTengId", value.f0);
                            map.put("updateTime", dateUtil1.setTimeZone(new Date()));
                            map.put("baiYunId", "");
                            map.put("travelPreference_holiday", value.f1);
                            out.collect(map);

                        }
                    });

            dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
            dataTransfer.print();
            System.out.println(dataTransfer.count());
        }


    }

}
