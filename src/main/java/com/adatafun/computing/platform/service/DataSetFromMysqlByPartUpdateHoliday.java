package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByPartUpdate;
import com.adatafun.computing.platform.util.HolidayUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateHoliday.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/19.
 */
public class DataSetFromMysqlByPartUpdateHoliday {

    public static void main(String[] args) throws Exception {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//HH:24小时制，hh:12小时制
        String minDate = simpleDateFormat.format(new Date());

        System.out.println(minDate);
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(simpleDateFormat.parse(minDate));
        String stopTime = simpleDateFormat.format(calendar.getTime());
        calendar.add(Calendar.YEAR, -1);//把日期往前减少1天.正数往后推,负数往前移动
        String startTime = simpleDateFormat.format(calendar.getTime());

        MysqlConf mysqlConf = new MysqlConf();
        DataSetInputFromMysqlParam dataSetInput = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl3(),
                mysqlConf.getUsername3(), mysqlConf.getPassword3(), mysqlConf.getSql3(), startTime, stopTime);
        List<Map> userList = dataSetInput.readFromMysqlByPartUpdate();
        System.out.println(userList.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Map> input = env.fromCollection(userList);

        DataSet<Tuple2<String, String>> dataGroup = input.flatMap(new FlatMapFunction<Map, Tuple2<String, String>>() {
            public void flatMap(Map value, Collector<Tuple2<String, String>> out) throws ParseException{

                HolidayUtil holidayUtil = new HolidayUtil();
                Tuple2<String, String> tuple2 = new Tuple2<>();

                String loginDate = value.get("createTime").toString();
                Date date = holidayUtil.getDate(loginDate);
                Boolean flag = holidayUtil.isHoliday(date);

                if (flag) {
                    tuple2.setFields(value.get("longTengId").toString(), "节假日出行");
                } else {
                    tuple2.setFields(value.get("longTengId").toString(), "非节假日出行");
                }

                out.collect(tuple2);

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
