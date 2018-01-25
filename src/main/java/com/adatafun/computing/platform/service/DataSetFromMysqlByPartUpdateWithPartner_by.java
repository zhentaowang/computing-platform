package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * DataSetFromMysqlByPartUpdateWithPartner_by.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/22.
 */
public class DataSetFromMysqlByPartUpdateWithPartner_by {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        String sql1 = "select open_id, client_phone from client_info where client_phone is not null;";
        String sql2 = "select mobile, service_time from ord_order_limousine where num > 1 and mobile is not null;";
        String sql3 = "select mobile, valid_date from ord_order_lounge where number > 1 and mobile is not null;";
        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql1);
        DataSetInputFromMysqlNoParam dataSetInput_order1 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql2);
        DataSetInputFromMysqlNoParam dataSetInput_order2 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql3);
        List<Tuple2<String, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByAES();
        List<Tuple2<String, String>> userList_order1 = dataSetInput_order1.dataEncapsulationTuple2ByPartner();
        List<Tuple2<String, String>> userList_order2 = dataSetInput_order2.dataEncapsulationTuple2ByPartner();
        System.out.println(userList_user.size() + "by connect successful");
        System.out.println(userList_order1.size() + "by connect successful");
        System.out.println(userList_order2.size() + "by connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, String>> input_order1 = env.fromCollection(userList_order1);
        DataSet<Tuple2<String, String>> input_order2 = env.fromCollection(userList_order2);
        DataSet<Tuple2<String, String>> input_order = input_order1.union(input_order2);

        DataSet<Tuple2<String, String>> input_user = env.fromCollection(userList_user);
//        DataSet<Tuple2<String, String>> input = input_user.join(input_order).where(1).equalTo(0)
//                .with(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
//                    public Tuple2<String, String> join(Tuple2<String, String> v1, Tuple2<String, String> v2) {
//                        Tuple2<String, String> tuple2 = new Tuple2<>();
//                        DateUtil holidayUtil = new DateUtil();
//                        Integer daySpace = holidayUtil.getDaySpace(v2.f1, "");
//                        if (daySpace <= 365) {
//                            tuple2.setFields(v1.f0, "近期结伴出行");
//                        } else {
//                            tuple2.setFields(v1.f0, "曾结伴出行");
//                        }
//                        return tuple2;
//                    }
//                });
//
//        input.print();
//        System.out.println(input.count());
//
//        DataSet<Map> dataTransfer = input.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//            public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException {
//
//                Map<String, Object> map = new HashMap<>();
//                map.put("baiYunId", value.f0);
//                map.put("longTengId", "");
//                map.put("travelPreference_withPartner", value.f1);
//                map.put("updateTime", new Date());
//                out.collect(map);
//
//            }
//        });
//
//        dataTransfer.print();
//        System.out.println(dataTransfer.count());
//        if (dataTransfer.count() > 0) {
//            dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
//            env.execute("travelPreference_withPartner");
//        }

    }

}
