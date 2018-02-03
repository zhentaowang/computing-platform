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
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateTravelDemand.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/20.
 */
public class DataSetFromMysqlByPartUpdateTravelDemand {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        List<String> sql_user = new ArrayList<>();
        List<String> sql_product = new ArrayList<>();
        sql_user.add("select user_id, flight_info_id from tbd_user_flight;");
        sql_user.add("select id, phone_str from tb_user where phone_str in (select phone from tb_order_limousine " +
                "where phone is not null and service_date > CURDATE());");
        sql_user.add("select id, phone_str from tb_user where phone_str in (select phone from tb_order_vvip " +
                "where phone is not null and service_date > CURDATE());");
        sql_user.add("select id, phone_str from tb_user where phone_str in (select phone from tb_order_loungeserv " +
                "where phone is not null and service_date > CURDATE());");
        sql_user.add("select id , name_str from tb_user where name_str in (select user_name from tb_order_parking " +
                "where user_name is not null and park_car_date > CURDATE());");
        sql_product.add("select id, depart_time_plan, TIMESTAMPDIFF(MINUTE,CURDATE(),depart_time_plan) as remaining_time " +
                "from tbd_flight_info where depart_time_plan > CURDATE();");
        sql_product.add("select phone, service_date, TIMESTAMPDIFF(MINUTE,CURDATE(),service_date) as remaining_time " +
                "from tb_order_limousine where phone is not null and service_date > CURDATE();");
        sql_product.add("select phone, service_date, TIMESTAMPDIFF(MINUTE,CURDATE(),service_date) as remaining_time " +
                "from tb_order_vvip where phone is not null and service_date > CURDATE();");
        sql_product.add("select phone, service_date, TIMESTAMPDIFF(MINUTE,CURDATE(),service_date) as remaining_time " +
                "from tb_order_loungeserv where phone is not null and service_date > CURDATE();");
        sql_product.add("select user_name, park_car_date, TIMESTAMPDIFF(MINUTE,CURDATE(),park_car_date) as remaining_time " +
                "from tb_order_parking where user_name is not null and park_car_date > CURDATE();");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, String>> dataUnion = null;

        for (int i = 0; i < sql_user.size(); i++ ) {

            DataSetInputFromMysqlNoParam dataSetInput_user_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                    mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql_user.get(i));
            DataSetInputFromMysqlNoParam dataSetInput_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                    mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql_product.get(i));

            List<Tuple2<Long, String>> userList_user = dataSetInput_user_flight.dataEncapsulationTuple2ByPartUpdate();
            List<Tuple3<String, String, String>> userList_product = dataSetInput_flight.dataEncapsulationTuple3ByPartUpdate();
            System.out.println(userList_user.size() + " lt connect successful");
            System.out.println(userList_product.size() + " lt connect successful");

            DataSet<Tuple2<Long, String>> input_user = env.fromCollection(userList_user);
            DataSet<Tuple3<String, String, String>> input_product = env.fromCollection(userList_product);

            DataSet<Tuple2<String, String>> input = input_user.join(input_product).where(1).equalTo(0)
                    .with(new JoinFunction<Tuple2<Long, String>, Tuple3<String, String, String>, Tuple2<String, String>>() {
                        public Tuple2<String, String> join(Tuple2<Long, String> v1, Tuple3<String, String, String> v2) {
                            Tuple2<String, String> tuple2 = new Tuple2<>();
                            tuple2.setFields(v1.f0.toString(), v2.f2);
                            return tuple2;
                        }
                    }).flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                        public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> out) throws ParseException {

                            LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                            out.collect(labelProcessingUtil.travelDemand(value, Integer.valueOf(value.f1)));

                        }
                    });

            if (i == 0) {
                dataUnion = input;
            } else {
                dataUnion.union(input);
            }
//            input.print();
//            System.out.println(input.count());
//            dataUnion.print();
//            System.out.println(dataUnion.count());

        }

        if (dataUnion != null) {

            DataSet<Map> dataTransfer = dataUnion.groupBy(0, 1).first(1).groupBy(0)
                    .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                        @Override
                        public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
                            DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                            dataEncapsulationUtil.doReduceGroup(values, out);
                        }
                    }).flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
                public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException{

                    DateUtil dateUtil = new DateUtil();
                    Map<String, Object> map = new HashMap<>();
                    map.put("longTengId", value.f0);
                    map.put("baiYunId", "");
                    String[] array = value.f1.split(",");
                    map.put("travelPreference_recentDemand", array);
                    if (value.f1.equals("非近期出行")) {
                        map.put("travelPreference_recentTravel", "非近期出行");
                    } else {
                        map.put("travelPreference_recentTravel", "近期出行");
                    }
                    map.put("updateTime", dateUtil.setTimeZone(new Date()));
                    out.collect(map);

                }
            });

            dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
            dataTransfer.print();
            System.out.println(dataTransfer.count());

        }


    }

}
