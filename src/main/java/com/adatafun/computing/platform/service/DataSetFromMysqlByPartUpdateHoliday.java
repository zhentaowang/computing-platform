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
        DataSet<Map> input = env.fromCollection(userList);

        DataSet<Tuple2<String, String>> dataGroup = input.flatMap(new FlatMapFunction<Map, Tuple2<String, String>>() {
            public void flatMap(Map value, Collector<Tuple2<String, String>> out) throws ParseException{
                LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                out.collect(labelProcessingUtil.holidayTravel(value));
            }
        }).groupBy(0).sortGroup(1, Order.ASCENDING).first(1);

        dataGroup.print();
        System.out.println(dataGroup.count());

        DataSet<Map> dataTransfer = dataGroup.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", value.getField(0));
                map.put("travelPreference_holiday", value.getField(1));
                out.collect(map);

            }
        });

        dataTransfer.print();
        System.out.println(dataTransfer.count());

        dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));
    }

}
