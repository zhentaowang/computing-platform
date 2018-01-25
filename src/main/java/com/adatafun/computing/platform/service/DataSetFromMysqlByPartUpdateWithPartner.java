package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

/**
 * DataSetFromMysqlByPartUpdateWithPartner.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/21.
 */
public class DataSetFromMysqlByPartUpdateWithPartner {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        String sql1 = "select id, phone_str from tb_user where phone_str is not null;";
        String sql2 = "select phone, service_date, person_num from tb_order_limousine where person_num > 1 and phone is not null;";
        String sql3 = "select phone, service_date, person_num from tb_order_vvip where person_num > 1 and phone is not null;";
        String sql4 = "select phone, service_date, num from tb_order_loungeserv where num > 1 and phone is not null;";
        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql1);
        DataSetInputFromMysqlNoParam dataSetInput_order1 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql2);
        DataSetInputFromMysqlNoParam dataSetInput_order2 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql3);
        DataSetInputFromMysqlNoParam dataSetInput_order3 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql4);
        List<Tuple2<Long, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByPartUpdate();
        List<Tuple3<String, String, String>> userList_order1 = dataSetInput_order1.dataEncapsulationTuple3ByPartUpdate();
        List<Tuple3<String, String, String>> userList_order2 = dataSetInput_order2.dataEncapsulationTuple3ByPartUpdate();
        List<Tuple3<String, String, String>> userList_order3 = dataSetInput_order3.dataEncapsulationTuple3ByPartUpdate();
        System.out.println(userList_user.size() + "lt connect successful");
        System.out.println(userList_order1.size() + "lt connect successful");
        System.out.println(userList_order2.size() + "lt connect successful");
        System.out.println(userList_order3.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<String, String, String>> input_order1 = env.fromCollection(userList_order1);
        DataSet<Tuple3<String, String, String>> input_order2  = env.fromCollection(userList_order2);
        DataSet<Tuple3<String, String, String>> input_order3 = env.fromCollection(userList_order3);
        DataSet<Tuple3<String, String, String>> input_order = input_order1.union(input_order2).union(input_order3);

//        Integer split = 1000000;
//        DataSet<Tuple2<String, String>> dataUnion = null;
//        Integer count = userList_user.size();
//        for (int i = 0; i < count/split+1; i++) {
//            Integer toIndex = count > (i+1)*split ? (i+1)*split : count;
//            DataSet<Tuple2<Long, String>> input_user = env.fromCollection(userList_user.subList(i*split, toIndex));
//            DataSet<Tuple2<String, String>> input = input_user.join(input_order).where(1).equalTo(0)
//                    .with(new JoinFunction<Tuple2<Long, String>, Tuple3<String, String, String>, Tuple2<String, String>>() {
//                        public Tuple2<String, String> join(Tuple2<Long, String> v1, Tuple3<String, String, String> v2) {
//                            Tuple2<String, String> tuple2 = new Tuple2<>();
//                            DateUtil holidayUtil = new DateUtil();
//                            Integer daySpace = holidayUtil.getDaySpace(v2.f1, "");
//                            if (daySpace <= 365) {
//                                tuple2.setFields(v1.f0.toString(), "近期结伴出行");
//                            } else {
//                                tuple2.setFields(v1.f0.toString(), "曾结伴出行");
//                            }
//                            return tuple2;
//                        }
//                    });
//
//            if (i == 0) {
//                dataUnion = input;
//            } else {
//                dataUnion.union(input);
//            }
//
//            input.print();
//            System.out.println(input.count());
//        }
//
//        if (dataUnion != null) {
//
//            DataSet<Map> dataTransfer = dataUnion.flatMap(new FlatMapFunction<Tuple2<String, String>, Map>() {
//        public void flatMap(Tuple2<String, String> value, Collector<Map> out) throws ParseException {
//
//            Map<String, Object> map = new HashMap<>();
//            map.put("longTengId", value.f0);
//            map.put("travelPreference_withPartner", value.f1);
//            out.collect(map);
//
//        }
//                    });
//
//        dataTransfer.print();
//        System.out.println(dataTransfer.count());
//        //        dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-ltuser", "dmp-ltuser"));
////            env.execute("travel_withPartner");
//
//        }

    }

}
