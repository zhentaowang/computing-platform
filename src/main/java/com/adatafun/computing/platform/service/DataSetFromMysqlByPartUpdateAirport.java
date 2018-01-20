package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import com.adatafun.computing.platform.util.DataEncapsulationUtil;
import com.adatafun.computing.platform.util.HolidayUtil;
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
 * DataSetFromMysqlByPartUpdateAirport.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/20.
 */
public class DataSetFromMysqlByPartUpdateAirport {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        String sql1 = "select user_id, flight_info_id from tbd_user_flight;";
        String sql2 = "select id, airport_depart_name, airport_arrive_name from tbd_flight_info;";
        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql1);
        DataSetInputFromMysqlNoParam dataSetInput_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql2);
        List<Tuple2<Long, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByPartUpdate();
        List<Tuple3<String, String, String>> userList_flight = dataSetInput_flight.dataEncapsulationTuple3ByPartUpdate();
        System.out.println(userList_user.size() + "lt connect successful");
        System.out.println(userList_flight.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, String>> input_user = env.fromCollection(userList_user);
        DataSet<Tuple3<String, String, String>> input_flight = env.fromCollection(userList_flight);

        DataSet<Tuple3<String, String, String>> input = input_user.join(input_flight).where(1).equalTo(0)
                .with(new JoinFunction<Tuple2<Long, String>, Tuple3<String, String, String>, Tuple3<String, String, String>>() {
                    public Tuple3<String, String, String> join(Tuple2<Long, String> v1, Tuple3<String, String, String> v2) {
                        Tuple3<String, String, String> tuple3 = new Tuple3<>();
                        tuple3.setFields(v1.f0.toString(), v2.f1, v2.f2);
                        return tuple3;
                    }
                });

        input.print();
        System.out.println(input.count());

        List<DataSet<Tuple3<String, String, Integer>>> dataGroups = new ArrayList<>();

        //常用起飞机场
        DataSet<Tuple3<String, String, Integer>> dataGroup0 = input.flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, Integer>>() {
            public void flatMap(Tuple3<String, String, String> value, Collector<Tuple3<String, String, Integer>> out) throws ParseException {

                Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
                tuple3.setFields(value.f0, value.f1, 1);
                out.collect(tuple3);

            }
        }).groupBy(0, 1).sum(2);

        dataGroups.add(dataGroup0);
        dataGroup0.print();
        System.out.println(dataGroup0.count());

        //常用降落机场
        DataSet<Tuple3<String, String, Integer>> dataGroup1 = input.flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, Integer>>() {
            public void flatMap(Tuple3<String, String, String> value, Collector<Tuple3<String, String, Integer>> out) throws ParseException {

                Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
                tuple3.setFields(value.f0, value.f2, 1);
                out.collect(tuple3);

            }
        }).groupBy(0, 1).sum(2);

        dataGroups.add(dataGroup1);
        dataGroup1.print();
        System.out.println(dataGroup1.count());

        List<DataSet<Tuple2<String, String>>> dataMerges = new ArrayList<>();

        for (DataSet<Tuple3<String, String, Integer>> dataGroup : dataGroups) {
            DataSet<Tuple2<String, String>> dataMerge = dataGroup.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
                public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException{

                    Integer count = value.f2;
                    Tuple2<String, String> tuple2 = new Tuple2<>();
                    if (count >= 6) {
                        tuple2.setFields(value.f0, value.f1);
                        out.collect(tuple2);
                    }
                }
            }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                @Override
                public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
                    DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                    dataEncapsulationUtil.doReduceGroup(values, out);
                }
            });

            dataMerges.add(dataMerge);
            dataMerge.print();
            System.out.println(dataMerge.count());

        }

        DataSet<Map> dataTransfer0 = dataMerges.get(0).flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", value.getField(0));
                map.put("travelPreference_departAirport", value.getField(1));
                out.collect(map);

            }
        });

        dataTransfer0.print();
        System.out.println(dataTransfer0.count());
        //        dataTransfer0.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));

        DataSet<Map> dataTransfer1 = dataMerges.get(1).flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", value.getField(0));
                map.put("travelPreference_arriveAirport", value.getField(1));
                out.collect(map);

            }
        });

        dataTransfer1.print();
        System.out.println(dataTransfer1.count());
        //        dataTransfer1.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));

    }

}
