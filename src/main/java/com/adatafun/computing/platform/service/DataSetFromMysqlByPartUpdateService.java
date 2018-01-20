package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByPartUpdate;
import com.adatafun.computing.platform.util.HolidayUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/18.
 */
public class DataSetFromMysqlByPartUpdateService {

    public static void main(String[] args) throws Exception {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//HH:24小时制，hh:12小时制
        String minDate = simpleDateFormat.format(new Date());

        System.out.println(minDate);
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(simpleDateFormat.parse(minDate));
        String stopTime = simpleDateFormat.format(calendar.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, -90);//把日期往前减少1天.正数往后推,负数往前移动
        String startTime = simpleDateFormat.format(calendar.getTime());

        MysqlConf mysqlConf = new MysqlConf();
        DataSetInputFromMysqlParam dataSetInput = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl3(),
                mysqlConf.getUsername3(), mysqlConf.getPassword3(), mysqlConf.getSql3(), startTime, stopTime);
        List<Map> userList = dataSetInput.readFromMysqlByPartUpdate();
        System.out.println(userList.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Map> input = env.fromCollection(userList);

        DataSet<Tuple3<String, String, Integer>> dataGroup = input.flatMap(new FlatMapFunction<Map, Tuple3<String, String, Integer>>() {
            public void flatMap(Map value, Collector<Tuple3<String, String, Integer>> out) throws ParseException{

                HolidayUtil holidayUtil = new HolidayUtil();
                Tuple3<String, String, Integer> tuple3 = new Tuple3<>();

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

        dataGroup.print();
        System.out.println(dataGroup.count());

        DataSet<Map> dataCount = dataGroup.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Map>() {
            public void flatMap(Tuple3<String, String, Integer> value, Collector<Map> out) throws ParseException{

                Map<String, Object> map = new HashMap<>();
                Integer count = value.getField(2);
                map.put("longTengId", value.getField(0));
                if (value.getField(1).equals("非周末出行")) {
                    if (count > 20) {
                        map.put("travelPreference_weekday", "经常非周末出行");
                    } else if (count > 3) {
                        map.put("travelPreference_weekday", "偶尔非周末出行");
                    } else {
                        map.put("travelPreference_weekday", "几乎没有非周末出行");
                    }
                    map.put("loginNum_weekday", value.getField(2));
                } else {
                    if (count > 15) {
                        map.put("travelPreference_weekend", "经常周末出行");
                    } else if (count > 3) {
                        map.put("travelPreference_weekend", "偶尔周末出行");
                    } else {
                        map.put("travelPreference_weekend", "几乎没有周末出行");
                    }
                    map.put("loginNum_weekend", value.getField(2));
                }
                out.collect(map);

            }
        });
        dataCount.print();
        System.out.println(dataCount.count());
        dataCount.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));
    }

}
