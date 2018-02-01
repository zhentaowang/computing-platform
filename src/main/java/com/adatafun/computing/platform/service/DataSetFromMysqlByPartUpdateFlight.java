package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByPartUpdate;
import com.adatafun.computing.platform.util.DataEncapsulationUtil;
import com.adatafun.computing.platform.util.DateUtil;
import com.adatafun.computing.platform.util.LabelProcessingUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateFlight.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/20.
 */
public class DataSetFromMysqlByPartUpdateFlight {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        String sql1 = "select user_id, flight_info_id from tbd_user_flight where DATE_SUB(CURDATE(),INTERVAL 180 DAY) <= create_time;";
        String sql2 = "select id, flight_category, depart_time_plan, TIMESTAMPDIFF(MINUTE,depart_time_plan,arrive_time_plan) as flight_time" +
                " from tbd_flight_info where id in (select flight_info_id from tbd_user_flight " +
                "where DATE_SUB(CURDATE(),INTERVAL 180 DAY) <= create_time);";
        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql1);
        DataSetInputFromMysqlNoParam dataSetInput_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql2);
        List<Tuple2<Long, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByPartUpdate();
        List<Tuple4<String, String, String, String>> userList_flight = dataSetInput_flight.dataEncapsulationTuple4ByPartUpdate();
        System.out.println(userList_user.size() + "lt connect successful");
        System.out.println(userList_flight.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, String>> input_user = env.fromCollection(userList_user);
        DataSet<Tuple4<String, String, String, String>> input_flight = env.fromCollection(userList_flight);

        DataSet<Map> input = input_user.join(input_flight).where(1).equalTo(0)
                .with(new JoinFunction<Tuple2<Long, String>, Tuple4<String, String, String, String>, Map>() {
            public Map join(Tuple2<Long, String> v1, Tuple4<String, String, String, String> v2) {
                v1.getField(0);
                Map<String, Object> map = new HashMap<>();
                map.put("user_id", v1.f0);
                map.put("flight_category", v2.f1);
                map.put("depart_time_plan", v2.f2);
                map.put("flight_time", v2.f3);
                return map;
            }
        });

//        input.print();
//        System.out.println(input.count());

        //航班时间偏好
        DataSet<Tuple3<String, Integer, Integer>> dataGroup0 = input.flatMap(new FlatMapFunction<Map, Tuple3<String, Integer, Integer>>() {
            public void flatMap(Map value, Collector<Tuple3<String, Integer, Integer>> out) throws ParseException {

                Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
                DateUtil holidayUtil = new DateUtil();

                String loginDate = value.get("depart_time_plan").toString();
                Date date = holidayUtil.getDateTime(loginDate);
                Integer flag = holidayUtil.matchTimeSlot(date);
                tuple3.setFields(value.get("user_id").toString(), flag, 1);

                out.collect(tuple3);

            }
        }).groupBy(0, 1).sum(2);

//        dataGroup0.print();
//        System.out.println(dataGroup0.count());

        DataSet<Tuple2<String, String>> dataMerge0 = dataGroup0.flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>>() {
            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{
                LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                Integer count = value.f2;
                if (count >= 3) {
                    out.collect(labelProcessingUtil.flightPreference(value));
                }
            }
        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
                DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                dataEncapsulationUtil.doReduceGroup(values, out);
            }
        });

//        dataMerge0.print();
//        System.out.println(dataMerge0.count());

        DataSet<Map> dataTransfer0 = dataMerge0.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                DateUtil dateUtil = new DateUtil();
                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", value.f0);
                map.put("updateTime", dateUtil.setTimeZone(new Date()));
                map.put("baiYunId", "");
                map.put("travelPreference_flightSchedule", value.f1);
                out.collect(map);

            }
        });

        dataTransfer0.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
        dataTransfer0.print();
        System.out.println(dataTransfer0.count());

        //飞行时间偏好
        DataSet<Tuple3<String, Integer, Integer>> dataGroup1 = input.flatMap(new FlatMapFunction<Map, Tuple3<String, Integer, Integer>>() {
            public void flatMap(Map value, Collector<Tuple3<String, Integer, Integer>> out) throws ParseException {

                Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
                Long flightTime = Long.valueOf(value.get("flight_time").toString());
                if (flightTime > 8*60) {
                    tuple3.setFields(value.get("user_id").toString(), 0, 1);
                } else if (flightTime > 3*60) {
                    tuple3.setFields(value.get("user_id").toString(), 1, 1);
                } else {
                    tuple3.setFields(value.get("user_id").toString(), 2, 1);
                }
                out.collect(tuple3);

            }
        }).groupBy(0, 1).sum(2);

//        dataGroup1.print();
//        System.out.println(dataGroup1.count());

        DataSet<Tuple2<String, String>> dataMerge1 = dataGroup1.flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>>() {
            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{
                LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                Integer count = value.f2;
                if (count >= 3) {
                    out.collect(labelProcessingUtil.flightDistance(value));
                }
            }
        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
                DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                dataEncapsulationUtil.doReduceGroup(values, out);
            }
        });

//        dataMerge1.print();
//        System.out.println(dataMerge1.count());

        DataSet<Map> dataTransfer1 = dataMerge1.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                DateUtil dateUtil = new DateUtil();
                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", value.f0);
                map.put("updateTime", dateUtil.setTimeZone(new Date()));
                map.put("baiYunId", "");
                map.put("travelPreference_flightDistance", value.f1);
                out.collect(map);

            }
        });

        dataTransfer1.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
        dataTransfer1.print();
        System.out.println(dataTransfer1.count());

        //国内外出行偏好
        DataSet<Tuple3<String, Integer, Integer>> dataGroup2 = input.flatMap(new FlatMapFunction<Map, Tuple3<String, Integer, Integer>>() {
            public void flatMap(Map value, Collector<Tuple3<String, Integer, Integer>> out) throws ParseException {

                Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
                String flightCategory = value.get("flight_category").toString();
                if (flightCategory.equals("0")) {
                    tuple3.setFields(value.get("user_id").toString(), 0, 1);
                } else {
                    tuple3.setFields(value.get("user_id").toString(), 1, 1);
                }
                out.collect(tuple3);

            }
        }).groupBy(0, 1).sum(2);

//        dataGroup2.print();
//        System.out.println(dataGroup2.count());

        DataSet<Tuple2<String, String>> dataMerge2 = dataGroup2.flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>>() {
            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{
                LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                Integer count = value.f2;
                if (count > 6) {
                    out.collect(labelProcessingUtil.flightSegment(value));
                }
            }
        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
                DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                dataEncapsulationUtil.doReduceGroup(values, out);
            }
        });

//        dataMerge2.print();
//        System.out.println(dataMerge2.count());

        DataSet<Map> dataTransfer2 = dataMerge2.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                DateUtil dateUtil = new DateUtil();
                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", value.f0);
                map.put("updateTime", dateUtil.setTimeZone(new Date()));
                map.put("baiYunId", "");
                map.put("travelPreference_flightSegment", value.f1);
                out.collect(map);

            }
        });

        dataTransfer2.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
        dataTransfer2.print();
        System.out.println(dataTransfer2.count());

    }

}
