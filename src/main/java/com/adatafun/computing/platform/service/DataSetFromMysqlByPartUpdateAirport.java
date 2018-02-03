package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
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

        Integer split = 100000;
        DataSet<Tuple3<String, String, Integer>> dataUnion_depart = null;
        DataSet<Tuple3<String, String, Integer>> dataUnion_arrive = null;
        Integer count = userList_user.size();
        for (int i = 0; i < count/split+1; i++) {
            Integer toIndex = count > (i+1)*split ? (i+1)*split : count;
            DataSet<Tuple2<Long, String>> input_user = env.fromCollection(userList_user.subList(i*split, toIndex));
            Integer count_flight = userList_flight.size();
            for (int j = 0; j < count_flight/split+1; j++) {
                Integer toIndex_flight = count_flight > (j+1)*split ? (j+1)*split : count_flight;
                DataSet<Tuple3<String, String, String>> input_flight = env.fromCollection(userList_flight.subList(j*split, toIndex_flight));
                DataSet<Tuple3<String, String, String>> input = input_user.join(input_flight).where(1).equalTo(0)
                        .with(new JoinFunction<Tuple2<Long, String>, Tuple3<String, String, String>, Tuple3<String, String, String>>() {
                            public Tuple3<String, String, String> join(Tuple2<Long, String> v1, Tuple3<String, String, String> v2) {
                                Tuple3<String, String, String> tuple3 = new Tuple3<>();
                                tuple3.setFields(v1.f0.toString(), v2.f1, v2.f2);
                                return tuple3;
                            }
                        });

                //常用起飞机场
                DataSet<Tuple3<String, String, Integer>> dataGroup_depart = input.flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, Integer>>() {
                    public void flatMap(Tuple3<String, String, String> value, Collector<Tuple3<String, String, Integer>> out) throws ParseException {

                        Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
                        tuple3.setFields(value.f0, value.f1, 1);
                        out.collect(tuple3);

                    }
                }).groupBy(0, 1).sum(2);

                //常用降落机场
                DataSet<Tuple3<String, String, Integer>> dataGroup_arrive = input.flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, Integer>>() {
                    public void flatMap(Tuple3<String, String, String> value, Collector<Tuple3<String, String, Integer>> out) throws ParseException {

                        Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
                        tuple3.setFields(value.f0, value.f2, 1);
                        out.collect(tuple3);

                    }
                }).groupBy(0, 1).sum(2);

                if (dataUnion_depart == null) {
                    dataUnion_depart = dataGroup_depart;
                } else {
                    dataUnion_depart.union(dataGroup_depart);
                }
                if (dataUnion_arrive == null) {
                    dataUnion_arrive = dataGroup_arrive;
                } else {
                    dataUnion_arrive.union(dataGroup_arrive);
                }

//                dataUnion_depart.print();
//                System.out.println(dataUnion_depart.count());
//                dataUnion_arrive.print();
//                System.out.println(dataUnion_arrive.count());
            }
        }

        if (dataUnion_depart != null) {
            LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();

            //常用起飞机场
            DataSet<Tuple3<String, String, Integer>> dataGroup0 = dataUnion_depart.groupBy(0, 1).sum(2);

//            dataGroup0.print();
//            System.out.println(dataGroup0.count());
            DataSet<Tuple2<String, String>> dataMerge0 = labelProcessingUtil.oftenUsedAirport(dataGroup0);

            DataSet<Map> dataTransfer0 = dataMerge0.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
                public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                    DateUtil dateUtil = new DateUtil();
                    Map<String, Object> map = new HashMap<>();
                    map.put("longTengId", value.f0);
                    map.put("updateTime", dateUtil.setTimeZone(new Date()));
                    map.put("baiYunId", "");
                    String[] array = value.f1.split(",");
                    map.put("travelPreference_departAirport", array);
                    out.collect(map);

                }
            });

            dataTransfer0.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
            dataTransfer0.print();
            System.out.println(dataTransfer0.count());
        }

        if (dataUnion_arrive != null) {
            LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();

            //常用降落机场
            DataSet<Tuple3<String, String, Integer>> dataGroup1 = dataUnion_arrive.groupBy(0, 1).sum(2);

//            dataGroup1.print();
//            System.out.println(dataGroup1.count());
            DataSet<Tuple2<String, String>> dataMerge1 = labelProcessingUtil.oftenUsedAirport(dataGroup1);

            DataSet<Map> dataTransfer1 = dataMerge1.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
                public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException {

                    DateUtil dateUtil = new DateUtil();
                    Map<String, Object> map = new HashMap<>();
                    map.put("longTengId", value.f0);
                    map.put("updateTime", dateUtil.setTimeZone(new Date()));
                    map.put("baiYunId", "");
                    String[] array = value.f1.split(",");
                    map.put("travelPreference_arriveAirport", array);
                    out.collect(map);

                }
            });

            dataTransfer1.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
            dataTransfer1.print();
            System.out.println(dataTransfer1.count());
        }

    }

}
