package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateTravelDemand_by.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/22.
 */
public class DataSetFromMysqlByPartUpdateTravelDemand_by {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        List<String> sql_user = new ArrayList<>();
        List<String> sql_product = new ArrayList<>();
        sql_user.add("select open_id, client_phone from client_info where client_phone is not null;");
        sql_product.add("select mobile, service_time from ord_order_limousine where mobile is not null;");
        sql_product.add("select mobile, valid_date from ord_order_lounge where mobile is not null;");
        sql_product.add("select mobile, park_car_time from ord_order_parking where mobile is not null;");

        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql_user.get(0));
        List<Tuple2<String, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByAES();
        System.out.println(userList_user.size() + " by connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, String>> input_user = env.fromCollection(userList_user);
        DataSet<Tuple2<String, String>> dataUnion = null;

//        for (int i = 0; i < sql_product.size(); i++ ) {
//
//            DataSetInputFromMysqlNoParam dataSetInput_product = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
//                    mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql_product.get(i));
//
//            List<Tuple2<String, String>> userList_product = dataSetInput_product.dataEncapsulationTuple2ByPartner();
//            System.out.println(userList_product.size() + " by connect successful");
//
//            DataSet<Tuple2<String, String>> input_product = env.fromCollection(userList_product);

//            DataSet<Tuple2<String, String>> input = input_user.join(input_product).where(1).equalTo(0)
//                    .with(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
//                        public Tuple2<String, String> join(Tuple2<String, String> v1, Tuple2<String, String> v2) {
//                            Tuple2<String, String> tuple2 = new Tuple2<>();
//                            tuple2.setFields(v1.f0, v2.f1);
//                            return tuple2;
//                        }
//                    }).flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
//                        public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> out) throws ParseException {
//
//                            Tuple2<String, String> tuple2 = new Tuple2<>();
//                            DateUtil holidayUtil = new DateUtil();
//                            Integer remainingTime = holidayUtil.getMinuteSpace(value.f1, "");
//                            if (remainingTime < 24*60) {
//                                tuple2.setFields(value.f0, "24小时内有出行需求");
//                            } else if (remainingTime < 48*60) {
//                                tuple2.setFields(value.f0, "48小时内有出行需求");
//                            } else if (remainingTime < 7*24*60) {
//                                tuple2.setFields(value.f0, "7天内有出行需求");
//                            } else if (remainingTime < 30*24*60) {
//                                tuple2.setFields(value.f0, "30天内有出行需求");
//                            } else {
//                                tuple2.setFields(value.f0, "非近期出行");
//                            }
//                            out.collect(tuple2);
//
//                        }
//                    });
//
//            if (i == 0) {
//                dataUnion = input;
//            } else {
//                dataUnion.union(input);
//            }
//            input.print();
//            dataUnion.print();
//            System.out.println(input.count());
//            System.out.println(dataUnion.count());
//
//        }
//
//        if (dataUnion != null) {
//
//            DataSet<Map> dataTransfer = dataUnion.groupBy(0, 1).first(1).groupBy(0)
//                    .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
//                        @Override
//                        public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
//                            DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
//                            dataEncapsulationUtil.doReduceGroup(values, out);
//                        }
//                    }).flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//                        public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{
//
//                            Map<String, Object> map = new HashMap<>();
//                            map.put("baiYunId", value.f0);
//                            map.put("longTengId", "");
//                            map.put("travelPreference_recentDemand", value.f1);
//                            if (value.f1.equals("非近期出行")) {
//                                map.put("travelPreference_recentTravel", "非近期出行");
//                            } else {
//                                map.put("travelPreference_recentTravel", "近期出行");
//                            }
//                            map.put("updateTime", new Date());
//                            out.collect(map);
//
//                        }
//                    });
//
//            if (dataTransfer.count() > 0) {
//                dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
//                env.execute("travelPreference_TravelDemand");
//            }
//            dataTransfer.print();
//            System.out.println(dataTransfer.count());
//
//        }


    }

}
