package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByPartUpdate;
import com.adatafun.computing.platform.util.DataEncapsulationUtil;
import com.adatafun.computing.platform.util.DateUtil;
import com.adatafun.computing.platform.util.LabelProcessingUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateTravelLabel_lt.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/24.
 */
public class DataSetFromMysqlByPartUpdateTravelLabel_lt {

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
        String sql1 = "select id, phone_str from tb_user where phone_str is not null;";
        String sql2 = "select phone, service_date, person_num from tb_order_limousine where person_num > 1 and phone is not null;";
        String sql3 = "select phone, service_date, person_num from tb_order_vvip where person_num > 1 and phone is not null;";
        String sql4 = "select phone, service_date, num from tb_order_loungeserv where num > 1 and phone is not null;";
        String sql5 = "select user_id, flight_info_id from tbd_user_flight where DATE_SUB(CURDATE(),INTERVAL 180 DAY) <= create_time;";
        String sql6 = "select id, flight_category, depart_time_plan, TIMESTAMPDIFF(MINUTE,depart_time_plan,arrive_time_plan) as flight_time" +
                " from tbd_flight_info where id in (select flight_info_id from tbd_user_flight " +
                "where DATE_SUB(CURDATE(),INTERVAL 180 DAY) <= create_time);";
        String sql7 = "select user_id, flight_info_id from tbd_user_flight;";
        String sql8 = "select id, airport_depart_name, airport_arrive_name from tbd_flight_info;";

        DateUtil dateUtil_week = new DateUtil();
        Map<String, String> map_weekend = dateUtil_week.getDateInterval(Calendar.DAY_OF_MONTH, -90);//把日期往前减90天
        String startTime_weekend = map_weekend.get("startTime");
        String stopTime_weekend = map_weekend.get("stopTime");

        Map<String, String> map_holiday = dateUtil_week.getDateInterval(Calendar.YEAR, -1);//把日期往前减1年
        String startTime_holiday = map_holiday.get("startTime");
        String stopTime_holiday = map_holiday.get("stopTime");

        //出行需求偏好
        DataSetInputFromMysqlNoParam dataSetInput_user_partner = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql1);
        DataSetInputFromMysqlNoParam dataSetInput_order1 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql2);
        DataSetInputFromMysqlNoParam dataSetInput_order2 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql3);
        DataSetInputFromMysqlNoParam dataSetInput_order3 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql4);
        List<Tuple2<Long, String>> userList_user_partner = dataSetInput_user_partner.dataEncapsulationTuple2ByPartUpdate();
        List<Tuple3<String, String, String>> userList_order1 = dataSetInput_order1.dataEncapsulationTuple3ByPartUpdate();
        List<Tuple3<String, String, String>> userList_order2 = dataSetInput_order2.dataEncapsulationTuple3ByPartUpdate();
        List<Tuple3<String, String, String>> userList_order3 = dataSetInput_order3.dataEncapsulationTuple3ByPartUpdate();

        //航班偏好
        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql5);
        DataSetInputFromMysqlNoParam dataSetInput_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql6);
        List<Tuple2<Long, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByPartUpdate();
        List<Tuple4<String, String, String, String>> userList_flight = dataSetInput_flight.dataEncapsulationTuple4ByPartUpdate();

        //出行日期偏好
        DataSetInputFromMysqlParam dataSetInput_weekend = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl3(),
                mysqlConf.getUsername3(), mysqlConf.getPassword3(), mysqlConf.getSql3(), startTime_weekend, stopTime_weekend);
        DataSetInputFromMysqlParam dataSetInput_holiday = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl3(),
                mysqlConf.getUsername3(), mysqlConf.getPassword3(), mysqlConf.getSql3(), startTime_holiday, stopTime_holiday);
        List<Map> userList_weekend = dataSetInput_weekend.readFromMysqlByPartUpdate();
        List<Map> userList_holiday = dataSetInput_holiday.readFromMysqlByPartUpdate();

        //常用机场
        DataSetInputFromMysqlNoParam dataSetInput_user_airport = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql7);
        DataSetInputFromMysqlNoParam dataSetInput_flight_airport = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql8);
        List<Tuple2<Long, String>> userList_user_airport = dataSetInput_user_airport.dataEncapsulationTuple2ByPartUpdate();
        List<Tuple3<String, String, String>> userList_flight_airport = dataSetInput_flight_airport.dataEncapsulationTuple3ByPartUpdate();

        System.out.println(userList_user.size() + "lt connect successful");
        System.out.println(userList_flight.size() + "lt connect successful");
        System.out.println(userList_user_partner.size() + "lt connect successful");
        System.out.println(userList_order1.size() + "lt connect successful");
        System.out.println(userList_order2.size() + "lt connect successful");
        System.out.println(userList_order3.size() + "lt connect successful");
        System.out.println(userList_weekend.size() + "lt connect successful");
        System.out.println(userList_holiday.size() + "lt connect successful");
        System.out.println(userList_user_airport.size() + "lt connect successful");
        System.out.println(userList_flight_airport.size() + "lt connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, String>> input_user = env.fromCollection(userList_user);
        DataSet<Tuple4<String, String, String, String>> input_flight = env.fromCollection(userList_flight);
        DataSet<Tuple3<String, String, String>> input_order1 = env.fromCollection(userList_order1);
        DataSet<Tuple3<String, String, String>> input_order2  = env.fromCollection(userList_order2);
        DataSet<Tuple3<String, String, String>> input_order3 = env.fromCollection(userList_order3);
        DataSet<Tuple3<String, String, String>> input_order = input_order1.union(input_order2).union(input_order3);
        DataSet<Map> input_weekend = env.fromCollection(userList_weekend);
        DataSet<Map> input_holiday = env.fromCollection(userList_holiday);
        DataSet<Tuple2<Long, String>> input_user_airport = env.fromCollection(userList_user_airport);
        DataSet<Tuple3<String, String, String>> input_flight_airport = env.fromCollection(userList_flight_airport);

        //出行需求
        DataSet<Tuple2<String, String>> dataUnion_demand = null;
        for (int i = 0; i < sql_user.size(); i++ ) {

            DataSetInputFromMysqlNoParam dataSetInput_user_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                    mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql_user.get(i));
            DataSetInputFromMysqlNoParam dataSetInput_flight_demand = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                    mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql_product.get(i));

            List<Tuple2<Long, String>> userList_user_demand = dataSetInput_user_flight.dataEncapsulationTuple2ByPartUpdate();
            List<Tuple3<String, String, String>> userList_product = dataSetInput_flight_demand.dataEncapsulationTuple3ByPartUpdate();
            System.out.println(userList_user.size() + " lt connect successful");
            System.out.println(userList_product.size() + " lt connect successful");

            DataSet<Tuple2<Long, String>> input_user_demand = env.fromCollection(userList_user_demand);
            DataSet<Tuple3<String, String, String>> input_product = env.fromCollection(userList_product);

            JoinFunction<Tuple2<Long, String>, Tuple3<String, String, String>, Tuple2<String, String>> joinFunction =
                    (Tuple2<Long, String> v1, Tuple3<String, String, String> v2) -> {
                        Tuple2<String, String> tuple2 = new Tuple2<>();
                        tuple2.setFields(v1.f0.toString(), v2.f2);
                        return tuple2;
            };
            FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> flatMapFunction =
                    (Tuple2<String, String> value, Collector<Tuple2<String, String>> out) -> {
                        LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                        out.collect(labelProcessingUtil.travelDemand(value, Integer.valueOf(value.f1)));
            };

            DataSet<Tuple2<String, String>> input_demand = input_user_demand.join(input_product).where(1).equalTo(0)
                    .with(joinFunction).returns(new TypeHint<Tuple2<String, String>>() {
                        @Override
                        public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    }).flatMap(flatMapFunction).returns(new TypeHint<Tuple2<String, String>>() {
                        @Override
                        public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    });

            if (i == 0) {
                dataUnion_demand = input_demand;
            } else {
                dataUnion_demand.union(input_demand);
            }
            input_demand.print();
            dataUnion_demand.print();
            System.out.println(input_demand.count());
            System.out.println(dataUnion_demand.count());

        }

        DataSet<Map> dataTransfer_demand;
        if (dataUnion_demand != null) {

            GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> groupReduceFunction =
                    (Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) -> {
                        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                        dataEncapsulationUtil.doReduceGroup(values, out);
            };
            FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction1 =
                    (Tuple2<String, String> value, Collector<Map> out) -> {
                        Map<String, Object> map = new HashMap<>();
                        map.put("longTengId", value.f0);
                        map.put("baiYunId", "");
                        map.put("updateTime", new Date());
                        map.put("travelPreference_recentDemand", value.f1);
                        if (value.f1.equals("非近期出行")) {
                            map.put("travelPreference_recentTravel", "非近期出行");
                        } else {
                            map.put("travelPreference_recentTravel", "近期出行");
                        }
                        out.collect(map);
            };

            dataTransfer_demand = dataUnion_demand.groupBy(0, 1).first(1).groupBy(0)
                    .reduceGroup(groupReduceFunction).returns(new TypeHint<Tuple2<String, String>>() {
                        @Override
                        public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    }).flatMap(flatMapFunction1).returns(new TypeHint<Map>() {
                        @Override
                        public TypeInformation<Map> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    });
            dataTransfer_demand.print();
            System.out.println(dataTransfer_demand.count());
        } else {
            dataTransfer_demand = null;
        }


        //航班偏好

        JoinFunction<Tuple2<Long, String>, Tuple4<String, String, String, String>, Map> joinFunction1 =
                (Tuple2<Long, String> v1, Tuple4<String, String, String, String> v2) -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("user_id", v1.f0);
                    map.put("flight_category", v2.f1);
                    map.put("depart_time_plan", v2.f2);
                    map.put("flight_time", v2.f3);
                    return map;
        };

        DataSet<Map> input = input_user.join(input_flight).where(1).equalTo(0)
                .with(joinFunction1).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        input.print();
        System.out.println(input.count());

        //航班时间偏好

        FlatMapFunction<Map, Tuple3<String, Integer, Integer>> flatMapFunction2 =
                (Map value, Collector<Tuple3<String, Integer, Integer>> out) -> {
                    DateUtil dateUtil = new DateUtil();
                    Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
                    String loginDate = value.get("depart_time_plan").toString();
                    Date date = dateUtil.getDateTime(loginDate);
                    Integer flag = dateUtil.matchTimeSlot(date);
                    tuple3.setFields(value.get("user_id").toString(), flag, 1);
                    out.collect(tuple3);
        };

        DataSet<Tuple3<String, Integer, Integer>> dataGroup0 = input
                .flatMap(flatMapFunction2).returns(new TypeHint<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public TypeInformation<Tuple3<String, Integer, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0, 1).sum(2);

        dataGroup0.print();
        System.out.println(dataGroup0.count());

        FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>> flatMapFunction3 =
                (Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) -> {
                    Integer count = value.f2;
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    if (count >= 3) {
                        out.collect(labelProcessingUtil.flightPreference(value));
                    }
        };
        GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> groupReduceFunction1 =
                (Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) -> {
                    DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                    dataEncapsulationUtil.doReduceGroup(values, out);
        };

        DataSet<Tuple2<String, String>> dataMerge0 = dataGroup0
                .flatMap(flatMapFunction3).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0)
                .reduceGroup(groupReduceFunction1).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataMerge0.print();
        System.out.println(dataMerge0.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction4 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("longTengId", value.f0);
            map.put("baiYunId", "");
            map.put("updateTime", new Date());
            map.put("travelPreference_flightSchedule", value.f1);
            out.collect(map);
        };

        DataSet<Map> dataTransfer0 = dataMerge0
                .flatMap(flatMapFunction4).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataTransfer0.print();
        System.out.println(dataTransfer0.count());

        //飞行时间偏好

        FlatMapFunction<Map, Tuple3<String, Integer, Integer>> flatMapFunction5 =
                (Map value, Collector<Tuple3<String, Integer, Integer>> out) -> {
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
        };

        DataSet<Tuple3<String, Integer, Integer>> dataGroup1 = input
                .flatMap(flatMapFunction5).returns(new TypeHint<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public TypeInformation<Tuple3<String, Integer, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0, 1).sum(2);

        dataGroup1.print();
        System.out.println(dataGroup1.count());

        FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>> flatMapFunction6 =
                (Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) -> {
                    Integer count = value.f2;
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    if (count >= 3) {
                        out.collect(labelProcessingUtil.flightDistance(value));
                    }
        };
        GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> groupReduceFunction2 =
                (Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) -> {
                    DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                    dataEncapsulationUtil.doReduceGroup(values, out);
        };

        DataSet<Tuple2<String, String>> dataMerge1 = dataGroup1
                .flatMap(flatMapFunction6).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0).reduceGroup(groupReduceFunction2).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataMerge1.print();
        System.out.println(dataMerge1.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction7 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("longTengId", value.f0);
            map.put("baiYunId", "");
            map.put("updateTime", new Date());
            map.put("travelPreference_flightDistance", value.f1);
            out.collect(map);
        };

        DataSet<Map> dataTransfer1 = dataMerge1
                .flatMap(flatMapFunction7).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataTransfer1.print();
        System.out.println(dataTransfer1.count());

        //国内外出行偏好

        FlatMapFunction<Map, Tuple3<String, Integer, Integer>> flatMapFunction8 =
                (Map value, Collector<Tuple3<String, Integer, Integer>> out) -> {
                    Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
                    String flightCategory = value.get("flight_category").toString();
                    if (flightCategory.equals("0")) {
                        tuple3.setFields(value.get("user_id").toString(), 0, 1);
                    } else {
                        tuple3.setFields(value.get("user_id").toString(), 1, 1);
                    }
                    out.collect(tuple3);
        };

        DataSet<Tuple3<String, Integer, Integer>> dataGroup2 = input
                .flatMap(flatMapFunction8).returns(new TypeHint<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public TypeInformation<Tuple3<String, Integer, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0, 1).sum(2);

        dataGroup2.print();
        System.out.println(dataGroup2.count());

        FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>> flatMapFunction9 =
                (Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) -> {
                    Integer count = value.f2;
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    if (count > 6) {
                        out.collect(labelProcessingUtil.flightSegment(value));
                    }
        };
        GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> groupReduceFunction3 =
                (Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) -> {
                    DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                    dataEncapsulationUtil.doReduceGroup(values, out);
        };

        DataSet<Tuple2<String, String>> dataMerge2 = dataGroup2
                .flatMap(flatMapFunction9).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0).reduceGroup(groupReduceFunction3).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataMerge2.print();
        System.out.println(dataMerge2.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction10 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("longTengId", value.f0);
            map.put("baiYunId", "");
            map.put("updateTime", new Date());
            map.put("travelPreference_flightSegment", value.f1);
            out.collect(map);
        };

        DataSet<Map> dataTransfer2 = dataMerge2
                .flatMap(flatMapFunction10).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataTransfer2.print();
        System.out.println(dataTransfer2.count());

        DataSet<Map> dataTransfer_flight = dataTransfer0.union(dataTransfer1).union(dataTransfer2);


        //携伴出行
        Integer split = 1000000;
        DataSet<Tuple2<String, String>> dataUnion_partner = null;
        Integer count = userList_user.size();
        for (int i = 0; i < count/split+1; i++) {
            Integer toIndex = count > (i+1)*split ? (i+1)*split : count;
            DataSet<Tuple2<Long, String>> input_user_partner = env.fromCollection(userList_user_partner.subList(i*split, toIndex));

            JoinFunction<Tuple2<Long, String>, Tuple3<String, String, String>, Tuple2<String, String>> joinFunction2 =
                    (Tuple2<Long, String> v1, Tuple3<String, String, String> v2) -> {
                        Tuple2<String, String> tuple2 = new Tuple2<>();
                        DateUtil dateUtil = new DateUtil();
                        Integer daySpace = dateUtil.getDaySpace(v2.f1, "");
                        if (daySpace <= 365) {
                            tuple2.setFields(v1.f0.toString(), "近期结伴出行");
                        } else {
                            tuple2.setFields(v1.f0.toString(), "曾结伴出行");
                        }
                        return tuple2;
            };

            DataSet<Tuple2<String, String>> input_partner = input_user_partner.join(input_order).where(1).equalTo(0)
                    .with(joinFunction2).returns(new TypeHint<Tuple2<String, String>>() {
                        @Override
                        public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    });

            if (i == 0) {
                dataUnion_partner = input_partner;
            } else {
                dataUnion_partner.union(input_partner);
            }

            input.print();
            System.out.println(input.count());
        }

        DataSet<Map> dataTransfer_partner;
        if (dataUnion_partner != null) {

            FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction11 = (Tuple2<String, String> value, Collector<Map> out) -> {
                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", value.f0);
                map.put("baiYunId", "");
                map.put("updateTime", new Date());
                map.put("travelPreference_withPartner", value.f1);
                out.collect(map);
            };

            dataTransfer_partner = dataUnion_partner
                    .flatMap(flatMapFunction11).returns(new TypeHint<Map>() {
                        @Override
                        public TypeInformation<Map> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    });

            dataTransfer_partner.print();
            System.out.println(dataTransfer_partner.count());

        } else {
            dataTransfer_partner = null;
        }

        //出行日期偏好
        DataSet<Map> dataTransfer_week;
        //周末出行
        FlatMapFunction<Map, Tuple3<String, String, Integer>> flatMapFunction12 =
                (Map value, Collector<Tuple3<String, String, Integer>> out) -> {
                    DateUtil dateUtil = new DateUtil();
                    Tuple3<String, String, Integer> tuple3 = new Tuple3<>();

                    String loginDate = value.get("createTime").toString();
                    Date date = dateUtil.getDate(loginDate);

                    if (dateUtil.isWeekend(date)) {
                        tuple3.setFields(value.get("longTengId").toString(), "周末出行", 1);
                    } else {
                        tuple3.setFields(value.get("longTengId").toString(), "非周末出行", 1);
                    }

                    out.collect(tuple3);
        };

        DataSet<Tuple3<String, String, Integer>> dataGroup_weekend = input_weekend
                .flatMap(flatMapFunction12).returns(new TypeHint<Tuple3<String, String, Integer>>() {
                    @Override
                    public TypeInformation<Tuple3<String, String, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0, 1).sum(2);

        dataGroup_weekend.print();
        System.out.println(dataGroup_weekend.count());

        FlatMapFunction<Tuple3<String, String, Integer>, Map> flatMapFunction13 =
                (Tuple3<String, String, Integer> value, Collector<Map> out) -> {
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    out.collect(labelProcessingUtil.weekendTravel(value));
        };

        DataSet<Map> dataCount_weekend = dataGroup_weekend
                .flatMap(flatMapFunction13).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataCount_weekend.print();
        System.out.println(dataCount_weekend.count());

        //节假日出行
        FlatMapFunction<Map, Tuple2<String, String>> flatMapFunction14 = (Map value, Collector<Tuple2<String, String>> out) -> {
            LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
            out.collect(labelProcessingUtil.holidayTravel(value));
        };

        DataSet<Tuple2<String, String>> dataGroup_holiday = input_holiday
                .flatMap(flatMapFunction14).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0).sortGroup(1, Order.ASCENDING).first(1);

        dataGroup_holiday.print();
        System.out.println(dataGroup_holiday.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction15 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("longTengId", value.f0);
            map.put("baiYunId", "");
            map.put("updateTime", new Date());
            map.put("travelPreference_holiday", value.f1);
            out.collect(map);
        };

        DataSet<Map> dataTransfer_holiday = dataGroup_holiday
                .flatMap(flatMapFunction15).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataTransfer_holiday.print();
        System.out.println(dataTransfer_holiday.count());
        dataTransfer_week = dataTransfer_holiday.union(dataCount_weekend);

        //常用机场
        JoinFunction<Tuple2<Long, String>, Tuple3<String, String, String>, Tuple3<String, String, String>> joinFunction3 =
                (Tuple2<Long, String> v1, Tuple3<String, String, String> v2) -> {
                    Tuple3<String, String, String> tuple3 = new Tuple3<>();
                    tuple3.setFields(v1.f0.toString(), v2.f1, v2.f2);
                    return tuple3;
        };

        DataSet<Tuple3<String, String, String>> input_airport = input_user_airport.join(input_flight_airport).where(1).equalTo(0)
                .with(joinFunction3).returns(new TypeHint<Tuple3<String, String, String>>() {
                    @Override
                    public TypeInformation<Tuple3<String, String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        input_airport.print();
        System.out.println(input_airport.count());

        List<DataSet<Tuple3<String, String, Integer>>> dataGroups_airport = new ArrayList<>();

        //常用起飞机场
        FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, Integer>> flatMapFunction16 =
                (Tuple3<String, String, String> value, Collector<Tuple3<String, String, Integer>> out) -> {
                    Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
                    tuple3.setFields(value.f0, value.f1, 1);
                    out.collect(tuple3);
        };

        DataSet<Tuple3<String, String, Integer>> dataGroup0_airport = input_airport
                .flatMap(flatMapFunction16).returns(new TypeHint<Tuple3<String, String, Integer>>() {
                    @Override
                    public TypeInformation<Tuple3<String, String, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0, 1).sum(2);

        dataGroups_airport.add(dataGroup0_airport);
        dataGroup0_airport.print();
        System.out.println(dataGroup0_airport.count());

        //常用降落机场
        FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, Integer>> flatMapFunction17 =
                (Tuple3<String, String, String> value, Collector<Tuple3<String, String, Integer>> out) -> {
                    Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
                    tuple3.setFields(value.f0, value.f2, 1);
                    out.collect(tuple3);
        };

        DataSet<Tuple3<String, String, Integer>> dataGroup1_airport = input_airport
                .flatMap(flatMapFunction17).returns(new TypeHint<Tuple3<String, String, Integer>>() {
                    @Override
                    public TypeInformation<Tuple3<String, String, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0, 1).sum(2);

        dataGroups_airport.add(dataGroup1_airport);
        dataGroup1_airport.print();
        System.out.println(dataGroup1_airport.count());

        LabelProcessingUtil labelProcessingUtil1 = new LabelProcessingUtil();
        List<DataSet<Tuple2<String, String>>> dataMerges_airport = labelProcessingUtil1.commonAirport(dataGroups_airport);

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction18 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("longTengId", value.f0);
            map.put("baiYunId", "");
            map.put("updateTime", new Date());
            map.put("travelPreference_departAirport", value.f1);
            out.collect(map);
        };

        DataSet<Map> dataTransfer0_airport = dataMerges_airport.get(0)
                .flatMap(flatMapFunction18).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataTransfer0_airport.print();
        System.out.println(dataTransfer0_airport.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction19 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("longTengId", value.f0);
            map.put("baiYunId", "");
            map.put("updateTime", new Date());
            map.put("travelPreference_arriveAirport", value.f1);
            out.collect(map);
        };

        DataSet<Map> dataTransfer1_airport = dataMerges_airport.get(1)
                .flatMap(flatMapFunction19);

        dataTransfer1_airport.print();
        System.out.println(dataTransfer1_airport.count());
        DataSet<Map> dataTransfer_airport = dataTransfer0_airport.union(dataTransfer1_airport);

        //数据合并输出
        List<DataSet<Map>> list = new ArrayList<>();
        list.add(dataTransfer_demand);
        list.add(dataTransfer_flight);
        list.add(dataTransfer_partner);
        list.add(dataTransfer_week);
        list.add(dataTransfer_airport);

        LabelProcessingUtil labelProcessingUtil2 = new LabelProcessingUtil();
        DataSet<Map> dataTransfer = labelProcessingUtil2.LabelDataMerge(list);

        if (dataTransfer != null) {
            dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
            dataTransfer.print();
            System.out.println(dataTransfer.count());
            env.execute("travelLabel_lt");
        }

    }

}
