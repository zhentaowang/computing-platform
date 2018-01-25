package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;

import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateFlight_by.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/22.
 */
public class DataSetFromMysqlByPartUpdateFlight_by {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        String sql1 = "select open_id, flight_no, dep_airport_code, arr_airport_code, dep_date from user_flight_rel;";
        String sql2 = "select dep_scheduled_date, arr_scheduled_date, dep_airport, arr_airport, board_in_out, flight_no," +
                " dep_airport_code, arr_airport_code, dep_date from flight_info where dep_scheduled_date is not null and " +
                "arr_scheduled_date is not null;";
        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql1);
        DataSetInputFromMysqlNoParam dataSetInput_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql2);
        List<Tuple2<String, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByString();
        List<Tuple6<String, String, String, String, String, String>> userList_flight = dataSetInput_flight
                .dataEncapsulationTuple6ByString();
        System.out.println(userList_user.size() + "by connect successful");
        System.out.println(userList_flight.size() + "by connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, String>> input_user = env.fromCollection(userList_user);
        DataSet<Tuple6<String, String, String, String, String, String>> input_flight = env.fromCollection(userList_flight);

//        DataSet<Tuple6<String, String, String, String, String, String>> input = input_user.join(input_flight)
//                .where(1).equalTo(5)
//                .with(new JoinFunction<Tuple2<String, String>, Tuple6<String, String, String, String, String, String>,
//                        Tuple6<String, String, String, String, String, String>>() {
//                    public Tuple6<String, String, String, String, String, String> join(
//                            Tuple2<String, String> v1, Tuple6<String, String, String, String, String, String> v2) {
//                        Tuple6<String, String, String, String, String, String> tuple6 = new Tuple6<>();
//                        tuple6.setFields(v1.f0, v2.f0, v2.f1, v2.f2, v2.f3, v2.f4);
//                        return tuple6;
//                    }
//                });
//
//        input.print();
//        System.out.println(input.count());
//
//        //近180天
//        DataSet<Tuple6<String, String, String, String, String, String>> input_copy = input.flatMap(
//                new FlatMapFunction<Tuple6<String, String, String, String, String, String>,
//                        Tuple6<String, String, String, String, String, String>>() {
//                    public void flatMap(Tuple6<String, String, String, String, String, String> value, Collector<Tuple6<String,
//                            String, String, String, String, String>> out)
//                            throws ParseException {
//
//                        DateUtil holidayUtil = new DateUtil();
//                        Tuple6<String, String, String, String, String, String> tuple6 = new Tuple6<>();
//                        Integer daySpace = holidayUtil.getDaySpace(value.f1, "");
//                        if (daySpace <= 180) {
//                            tuple6.setFields(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5);
//                            out.collect(tuple6);
//                        }
//
//                    }
//                });
//
//        input_copy.print();
//        System.out.println(input_copy.count());
//
//        //航班时间偏好
//        DataSet<Tuple3<String, Integer, Integer>> dataGroup0 = input_copy.flatMap(new FlatMapFunction<Tuple6<String, String,
//                String, String, String, String>, Tuple3<String, Integer, Integer>>() {
//            public void flatMap(Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, Integer, Integer>> out)
//                    throws ParseException {
//
//                DateUtil holidayUtil = new DateUtil();
//                Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
//                String loginDate = value.f1;
//                Date date = holidayUtil.getDateTime(loginDate);
//                Integer flag = holidayUtil.matchTimeSlot(date);
//                tuple3.setFields(value.f0, flag, 1);
//
//                out.collect(tuple3);
//
//            }
//        }).groupBy(0, 1).sum(2);
//
//        dataGroup0.print();
//        System.out.println(dataGroup0.count());
//
//        DataSet<Tuple2<String, String>> dataMerge0 = dataGroup0.flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>>() {
//            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{
//
//                Integer type = value.f1;
//                Integer count = value.f2;
//                Tuple2<String, String> tuple2 = new Tuple2<>();
//                if (count >= 3) {
//                    switch (type) {
//                        case 0:
//                            tuple2.setFields(value.f0, "红眼航班");
//                            break;
//                        case 1:
//                            tuple2.setFields(value.f0, "早上班机");
//                            break;
//                        case 2:
//                            tuple2.setFields(value.f0, "中午班机");
//                            break;
//                        case 3:
//                            tuple2.setFields(value.f0, "下午班机");
//                            break;
//                        case 4:
//                            tuple2.setFields(value.f0, "晚上航班");
//                            break;
//                    }
//                    out.collect(tuple2);
//                }
//            }
//        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
//            @Override
//            public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
//                DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
//                dataEncapsulationUtil.doReduceGroup(values, out);
//            }
//        });
//
//        dataMerge0.print();
//        System.out.println(dataMerge0.count());
//
//        DataSet<Map> dataTransfer0 = dataMerge0.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{
//
//                Map<String, Object> map = new HashMap<>();
//                map.put("baiYunId", value.f0);
//                map.put("longTengId", "");
//                map.put("travelPreference_flightSchedule", value.f1);
//                map.put("updateTime", new Date());
//                out.collect(map);
//
//            }
//        });
//
//        dataTransfer0.print();
//        System.out.println(dataTransfer0.count());
//        //        dataTransfer0.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));
//
//        //飞行时间偏好
//        DataSet<Tuple3<String, Integer, Integer>> dataGroup1 = input_copy.flatMap(new FlatMapFunction<Tuple6<String, String,
//                String, String, String, String>, Tuple3<String, Integer, Integer>>() {
//            public void flatMap(Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, Integer, Integer>> out)
//                    throws ParseException {
//
//                Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
//                DateUtil holidayUtil = new DateUtil();
//                Integer flightTime = holidayUtil.getMinuteSpace(value.f1, value.f2);
//                if (flightTime > 8*60) {
//                    tuple3.setFields(value.f0, 0, 1);
//                } else if (flightTime > 3*60) {
//                    tuple3.setFields(value.f0, 1, 1);
//                } else {
//                    tuple3.setFields(value.f0, 2, 1);
//                }
//                out.collect(tuple3);
//
//            }
//        }).groupBy(0, 1).sum(2);
//
//        dataGroup1.print();
//        System.out.println(dataGroup1.count());
//
//        DataSet<Tuple2<String, String>> dataMerge1 = dataGroup1.flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>>() {
//            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{
//
//                Integer type = value.f1;
//                Integer count = value.f2;
//                Tuple2<String, String> tuple2 = new Tuple2<>();
//                if (count >= 3) {
//                    switch (type) {
//                        case 0:
//                            tuple2.setFields(value.f0, "长途航班");
//                            break;
//                        case 1:
//                            tuple2.setFields(value.f0, "中途航班");
//                            break;
//                        case 2:
//                            tuple2.setFields(value.f0, "短途航班");
//                            break;
//                    }
//                    out.collect(tuple2);
//                }
//            }
//        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
//            @Override
//            public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
//                DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
//                dataEncapsulationUtil.doReduceGroup(values, out);
//            }
//        });
//
//        dataMerge1.print();
//        System.out.println(dataMerge1.count());
//
//        DataSet<Map> dataTransfer1 = dataMerge1.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{
//
//                Map<String, Object> map = new HashMap<>();
//                map.put("baiYunId", value.f0);
//                map.put("longTengId", "");
//                map.put("travelPreference_flightDistance", value.f1);
//                map.put("updateTime", new Date());
//                out.collect(map);
//
//            }
//        });
//
//        dataTransfer1.print();
//        System.out.println(dataTransfer1.count());
//        //        dataTransfer1.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));
//
//        //国内外出行偏好
//        DataSet<Tuple3<String, Integer, Integer>> dataGroup2 = input_copy.flatMap(new FlatMapFunction<Tuple6<String, String,
//                String, String, String, String>, Tuple3<String, Integer, Integer>>() {
//            public void flatMap(Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, Integer, Integer>> out)
//                    throws ParseException {
//
//                Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
//                String flightCategory = value.f4;
//                if (flightCategory.equals("1")) {
//                    tuple3.setFields(value.f0, 0, 1);
//                } else {
//                    tuple3.setFields(value.f0, 1, 1);
//                }
//                out.collect(tuple3);
//
//            }
//        }).groupBy(0, 1).sum(2);
//
//        dataGroup2.print();
//        System.out.println(dataGroup2.count());
//
//        DataSet<Tuple2<String, String>> dataMerge2 = dataGroup2.flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>>() {
//            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{
//
//                Integer type = value.f1;
//                Integer count = value.f2;
//                Tuple2<String, String> tuple2 = new Tuple2<>();
//                if (count > 6) {
//                    switch (type) {
//                        case 0:
//                            tuple2.setFields(value.f0, "国内航段偏好");
//                            break;
//                        case 1:
//                            tuple2.setFields(value.f0, "国际航段偏好");
//                            break;
//                    }
//                    out.collect(tuple2);
//                }
//            }
//        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
//            @Override
//            public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
//                DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
//                dataEncapsulationUtil.doReduceGroup(values, out);
//            }
//        });
//
//        dataMerge2.print();
//        System.out.println(dataMerge2.count());
//
//        DataSet<Map> dataTransfer2 = dataMerge2.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{
//
//                Map<String, Object> map = new HashMap<>();
//                map.put("baiYunId", value.f0);
//                map.put("longTengId", "");
//                map.put("travelPreference_flightSegment", value.f1);
//                map.put("updateTime", new Date());
//                out.collect(map);
//
//            }
//        });
//
//        dataTransfer2.print();
//        System.out.println(dataTransfer2.count());
////        dataTransfer2.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));
//
//        List<DataSet<Tuple3<String, String, Integer>>> dataGroups = new ArrayList<>();
//
//        //常用起飞机场
//        DataSet<Tuple3<String, String, Integer>> dataGroup3 = input.flatMap(new FlatMapFunction<Tuple6<String, String,
//                String, String, String, String>, Tuple3<String, String, Integer>>() {
//            public void flatMap(Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, String, Integer>> out)
//                    throws ParseException {
//
//                Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
//                tuple3.setFields(value.f0, value.f3, 1);
//                out.collect(tuple3);
//
//            }
//        }).groupBy(0, 1).sum(2);
//
//        dataGroups.add(dataGroup3);
//        dataGroup3.print();
//        System.out.println(dataGroup3.count());
//
//        //常用降落机场
//        DataSet<Tuple3<String, String, Integer>> dataGroup4 = input.flatMap(new FlatMapFunction<Tuple6<String, String,
//                String, String, String, String>, Tuple3<String, String, Integer>>() {
//            public void flatMap(Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, String, Integer>> out)
//                    throws ParseException {
//
//                Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
//                tuple3.setFields(value.f0, value.f4, 1);
//                out.collect(tuple3);
//
//            }
//        }).groupBy(0, 1).sum(2);
//
//        dataGroups.add(dataGroup4);
//        dataGroup4.print();
//        System.out.println(dataGroup4.count());
//
//        List<DataSet<Tuple2<String, String>>> dataMerges = new ArrayList<>();
//
//        for (DataSet<Tuple3<String, String, Integer>> dataGroup : dataGroups) {
//            DataSet<Tuple2<String, String>> dataMerge = dataGroup.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
//                public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{
//
//                    Integer count = value.f2;
//                    Tuple2<String, String> tuple2 = new Tuple2<>();
//                    if (count >= 6) {
//                        tuple2.setFields(value.f0, value.f1);
//                        out.collect(tuple2);
//                    }
//                }
//            }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
//                @Override
//                public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
//                    DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
//                    dataEncapsulationUtil.doReduceGroup(values, out);
//                }
//            });
//
//            dataMerges.add(dataMerge);
//            dataMerge.print();
//            System.out.println(dataMerge.count());
//
//        }
//
//        DataSet<Map> dataTransfer3 = dataMerges.get(0).flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{
//
//                Map<String, Object> map = new HashMap<>();
//                map.put("baiYunId", value.f0);
//                map.put("longTengId", "");
//                map.put("travelPreference_departAirport", value.f1);
//                map.put("updateTime", new Date());
//                out.collect(map);
//
//            }
//        });
//
//        dataTransfer3.print();
//        System.out.println(dataTransfer3.count());
////        if (dataTransfer3.count() > 0) {
////            dataTransfer3.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
////        }
//
//        DataSet<Map> dataTransfer4 = dataMerges.get(1).flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{
//
//                Map<String, Object> map = new HashMap<>();
//                map.put("baiYunId", value.f0);
//                map.put("longTengId", "");
//                map.put("travelPreference_arriveAirport", value.f1);
//                map.put("updateTime", new Date());
//                out.collect(map);
//
//            }
//        });
//
////        if (dataTransfer4.count() > 0) {
////            dataTransfer4.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
////        }
//
//        dataTransfer4.print();
//        System.out.println(dataTransfer4.count());
//
//        DataSet<Map> dataTransfer = dataTransfer0.union(dataTransfer1).union(dataTransfer2).union(dataTransfer3).union(dataTransfer4);
//
//        if (dataTransfer.count() > 0) {
//            dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
//            env.execute("travelPreference_flight");
//        }
//
//        dataTransfer.print();
//        System.out.println(dataTransfer.count());

    }

}
