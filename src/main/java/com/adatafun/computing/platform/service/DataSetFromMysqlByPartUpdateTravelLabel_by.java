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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * DataSetFromMysqlByPartUpdateTravelLabel_by.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/23.
 */
public class DataSetFromMysqlByPartUpdateTravelLabel_by {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        List<String> sql_product = new ArrayList<>();
        sql_product.add("select mobile, service_time from ord_order_limousine where mobile is not null;");
        sql_product.add("select mobile, valid_date from ord_order_lounge where mobile is not null;");
        sql_product.add("select mobile, park_car_time from ord_order_parking where mobile is not null;");

        String sql1 = "select open_id, client_phone from client_info where client_phone is not null;";
        String sql2 = "select mobile, service_time from ord_order_limousine where num > 1 and mobile is not null;";
        String sql3 = "select mobile, valid_date from ord_order_lounge where number > 1 and mobile is not null;";
        String sql4 = "select open_id, flight_no, dep_airport_code, arr_airport_code, dep_date from user_flight_rel;";
        String sql5 = "select dep_scheduled_date, arr_scheduled_date, dep_airport, arr_airport, board_in_out, flight_no," +
                " dep_airport_code, arr_airport_code, dep_date from flight_info where dep_scheduled_date is not null and " +
                "arr_scheduled_date is not null;";

        DataSetInputFromMysqlNoParam dataSetInput_user = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql1);
        DataSetInputFromMysqlNoParam dataSetInput_order1 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql2);
        DataSetInputFromMysqlNoParam dataSetInput_order2 = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql3);
        DataSetInputFromMysqlNoParam dataSetInput_user_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql4);
        DataSetInputFromMysqlNoParam dataSetInput_flight = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql5);

        List<Tuple2<String, String>> userList_user = dataSetInput_user.dataEncapsulationTuple2ByAES();
        List<Tuple2<String, String>> userList_order1 = dataSetInput_order1.dataEncapsulationTuple2ByPartner();
        List<Tuple2<String, String>> userList_order2 = dataSetInput_order2.dataEncapsulationTuple2ByPartner();
        List<Tuple2<String, String>> userList_user_flight = dataSetInput_user_flight.dataEncapsulationTuple2ByString();
        List<Tuple6<String, String, String, String, String, String>> userList_flight = dataSetInput_flight
                .dataEncapsulationTuple6ByString();

        System.out.println(userList_user.size() + "by connect successful");
        System.out.println(userList_order1.size() + "by connect successful");
        System.out.println(userList_order2.size() + "by connect successful");
        System.out.println(userList_user_flight.size() + "by connect successful");
        System.out.println(userList_flight.size() + "by connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, String>> input_user = env.fromCollection(userList_user);

        DataSet<Tuple2<String, String>> input_order1 = env.fromCollection(userList_order1);
        DataSet<Tuple2<String, String>> input_order2 = env.fromCollection(userList_order2);
        DataSet<Tuple2<String, String>> input_order = input_order1.union(input_order2);

        DataSet<Map> dataTransfer = null;//结果集

        JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> joinFunction =
                (Tuple2<String, String> v1, Tuple2<String, String> v2) -> {
                    Tuple2<String, String> tuple2 = new Tuple2<>();
                    DateUtil dateUtil = new DateUtil();
                    Integer daySpace = dateUtil.getDaySpace(v2.f1, "");
                    if (daySpace <= 365) {
                        tuple2.setFields(v1.f0, "近期结伴出行");
                    } else {
                        tuple2.setFields(v1.f0, "曾结伴出行");
                    }
                    return tuple2;
                };

        //结伴出行偏好
        DataSet<Tuple2<String, String>> input_withPartner = input_user.join(input_order).where(1).equalTo(0)
                .with(joinFunction).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        input_withPartner.print();
        System.out.println(input_withPartner.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction = (Tuple2<String, String> value, Collector<Map> out)
        -> {
            Map<String, Object> map = new HashMap<>();
            map.put("baiYunId", value.f0);
            map.put("longTengId", "");
            map.put("travelPreference_withPartner", value.f1);
            map.put("updateTime", new Date());
            out.collect(map);
        };

        DataSet<Map> dataTransfer_withPartner = input_withPartner
                .flatMap(flatMapFunction).returns(new TypeHint<Map>() {
                    @Override
                    public TypeInformation<Map> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        if (dataTransfer_withPartner.count() != 0) {
            dataTransfer = dataTransfer_withPartner;
        }
        dataTransfer_withPartner.print();
        System.out.println(dataTransfer_withPartner.count());

        //出行需求偏好
        DataSet<Tuple2<String, String>> dataUnion = null;
        for (int i = 0; i < sql_product.size(); i++ ) {

            DataSetInputFromMysqlNoParam dataSetInput_product = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                    mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql_product.get(i));

            List<Tuple2<String, String>> userList_product = dataSetInput_product.dataEncapsulationTuple2ByPartner();
            System.out.println(userList_product.size() + " by connect successful");

            DataSet<Tuple2<String, String>> input_product = env.fromCollection(userList_product);

            JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> joinFunction1 =
                    (Tuple2<String, String> v1, Tuple2<String, String> v2) -> {
                        Tuple2<String, String> tuple2 = new Tuple2<>();
                        tuple2.setFields(v1.f0, v2.f1);
                        return tuple2;
            };

            FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> flatMapFunction1 =
                    (Tuple2<String, String> value, Collector<Tuple2<String, String>> out) -> {
                        DateUtil dateUtil = new DateUtil();
                        LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                        Integer remainingTime = dateUtil.getMinuteSpace(value.f1, "");
                        out.collect(labelProcessingUtil.travelDemand(value, remainingTime));
            };

            DataSet<Tuple2<String, String>> input_travelDemand = input_user.join(input_product).where(1).equalTo(0)
                    .with(joinFunction1).returns(new TypeHint<Tuple2<String, String>>() {
                        @Override
                        public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    })
                    .flatMap(flatMapFunction1).returns(new TypeHint<Tuple2<String, String>>() {
                        @Override
                        public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    });

            if (i == 0) {
                dataUnion = input_travelDemand;
            } else {
                dataUnion.union(input_travelDemand);
            }
            input_travelDemand.print();
            dataUnion.print();
            System.out.println(input_travelDemand.count());
            System.out.println(dataUnion.count());

        }

        DataSet<Map> dataTransfer_travelDemand;
        if (dataUnion != null) {

            GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> groupReduceFunction =
                    (Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) -> {
                        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                        dataEncapsulationUtil.doReduceGroup(values, out);
            };

            FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction2 =
                    (Tuple2<String, String> value, Collector<Map> out) -> {
                        Map<String, Object> map = new HashMap<>();
                        map.put("baiYunId", value.f0);
                        map.put("longTengId", "");
                        map.put("travelPreference_recentDemand", value.f1);
                        if (value.f1.equals("非近期出行")) {
                            map.put("travelPreference_recentTravel", "非近期出行");
                        } else {
                            map.put("travelPreference_recentTravel", "近期出行");
                        }
                        map.put("updateTime", new Date());
                        out.collect(map);
            };

            dataTransfer_travelDemand = dataUnion.groupBy(0, 1).first(1).groupBy(0)
                    .reduceGroup(groupReduceFunction).returns(new TypeHint<Tuple2<String, String>>() {
                        @Override
                        public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    })
                    .flatMap(flatMapFunction2).returns(new TypeHint<Map>() {
                        @Override
                        public TypeInformation<Map> getTypeInfo() {
                            return super.getTypeInfo();
                        }
                    });

            dataTransfer_travelDemand.print();
            System.out.println(dataTransfer_travelDemand.count());

            if (dataTransfer_travelDemand.count() != 0) {
                if (dataTransfer == null) {
                    dataTransfer = dataTransfer_travelDemand;
                } else {
                    dataTransfer = dataTransfer.union(dataTransfer_travelDemand);
                }
            }
        }

        //航班偏好
        DataSet<Tuple2<String, String>> input_user_flight = env.fromCollection(userList_user_flight);
        DataSet<Tuple6<String, String, String, String, String, String>> input_flight = env.fromCollection(userList_flight);

        JoinFunction<Tuple2<String, String>, Tuple6<String, String, String, String, String, String>,
                Tuple6<String, String, String, String, String, String>> joinFunction3 =
                (Tuple2<String, String> v1, Tuple6<String, String, String, String, String, String> v2) -> {
                    Tuple6<String, String, String, String, String, String> tuple6 = new Tuple6<>();
                    tuple6.setFields(v1.f0, v2.f0, v2.f1, v2.f2, v2.f3, v2.f4);
                    return tuple6;
        };

        DataSet<Tuple6<String, String, String, String, String, String>> input = input_user_flight.join(input_flight)
                .where(1).equalTo(5)
                .with(joinFunction3).returns(new TypeHint<Tuple6<String, String, String, String, String, String>>() {
                    @Override
                    public TypeInformation<Tuple6<String, String, String, String, String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        input.print();
        System.out.println(input.count());

        //近180天

        FlatMapFunction<Tuple6<String, String, String, String, String, String>,
                Tuple6<String, String, String, String, String, String>> flatMapFunction3 =
                (Tuple6<String, String, String, String, String, String> value, Collector<Tuple6<String, String, String, String, String, String>> out) -> {
                    DateUtil dateUtil = new DateUtil();
                    Tuple6<String, String, String, String, String, String> tuple6 = new Tuple6<>();
                    Integer daySpace = dateUtil.getDaySpace(value.f1, "");
                    if (daySpace <= 180) {
                        tuple6.setFields(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5);
                        out.collect(tuple6);
                    }
        };

        DataSet<Tuple6<String, String, String, String, String, String>> input_copy = input
                .flatMap(flatMapFunction3).returns(new TypeHint<Tuple6<String, String, String, String, String, String>>() {
                    @Override
                    public TypeInformation<Tuple6<String, String, String, String, String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        input_copy.print();
        System.out.println(input_copy.count());

        //航班时间偏好

        FlatMapFunction<Tuple6<String, String,
                String, String, String, String>, Tuple3<String, Integer, Integer>> flatMapFunction4 =
                (Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, Integer, Integer>> out) -> {
                    DateUtil dateUtil = new DateUtil();
                    Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
                    String loginDate = value.f1;
                    Date date = dateUtil.getDateTime(loginDate);
                    Integer flag = dateUtil.matchTimeSlot(date);
                    tuple3.setFields(value.f0, flag, 1);
                    out.collect(tuple3);
        };

        DataSet<Tuple3<String, Integer, Integer>> dataGroup0 = input_copy
                .flatMap(flatMapFunction4).returns(new TypeHint<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public TypeInformation<Tuple3<String, Integer, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0, 1).sum(2);

        dataGroup0.print();
        System.out.println(dataGroup0.count());

        FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>> flatMapFunction5 =
                (Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) -> {
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    Integer count = value.f2;
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
                .flatMap(flatMapFunction5).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                }).groupBy(0).reduceGroup(groupReduceFunction1).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataMerge0.print();
        System.out.println(dataMerge0.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction6 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("baiYunId", value.f0);
            map.put("longTengId", "");
            map.put("travelPreference_flightSchedule", value.f1);
            map.put("updateTime", new Date());
            out.collect(map);
        };

        DataSet<Map> dataTransfer0 = dataMerge0.flatMap(flatMapFunction6).returns(new TypeHint<Map>() {
            @Override
            public TypeInformation<Map> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        dataTransfer0.print();
        System.out.println(dataTransfer0.count());

        //飞行时间偏好

        FlatMapFunction<Tuple6<String, String, String, String, String, String>, Tuple3<String, Integer, Integer>>
                flatMapFunction7 = (Tuple6<String, String, String, String, String, String> value,
                                    Collector<Tuple3<String, Integer, Integer>> out) -> {
            Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
            DateUtil dateUtil = new DateUtil();
            Integer flightTime = dateUtil.getMinuteSpace(value.f1, value.f2);
            if (flightTime > 8*60) {
                tuple3.setFields(value.f0, 0, 1);
            } else if (flightTime > 3*60) {
                tuple3.setFields(value.f0, 1, 1);
            } else {
                tuple3.setFields(value.f0, 2, 1);
            }
            out.collect(tuple3);
        };

        DataSet<Tuple3<String, Integer, Integer>> dataGroup1 = input_copy.flatMap(flatMapFunction7).returns(new TypeHint<Tuple3<String, Integer, Integer>>() {
            @Override
            public TypeInformation<Tuple3<String, Integer, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).groupBy(0, 1).sum(2);

        dataGroup1.print();
        System.out.println(dataGroup1.count());

        FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>> flatMapFunction8 =
                (Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) -> {
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    Integer count = value.f2;
                    if (count >= 3) {
                        out.collect(labelProcessingUtil.flightDistance(value));
                    }
        };

        GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> groupReduceFunction2 =
                (Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) -> {
                    DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
                    dataEncapsulationUtil.doReduceGroup(values, out);
        };

        DataSet<Tuple2<String, String>> dataMerge1 = dataGroup1.flatMap(flatMapFunction8).returns(new TypeHint<Tuple2<String, String>>() {
            @Override
            public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).groupBy(0)
                .reduceGroup(groupReduceFunction2).returns(new TypeHint<Tuple2<String, String>>() {
                    @Override
                    public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        dataMerge1.print();
        System.out.println(dataMerge1.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction9 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("baiYunId", value.f0);
            map.put("longTengId", "");
            map.put("travelPreference_flightDistance", value.f1);
            map.put("updateTime", new Date());
            out.collect(map);
        };

        DataSet<Map> dataTransfer1 = dataMerge1.flatMap(flatMapFunction9).returns(new TypeHint<Map>() {
            @Override
            public TypeInformation<Map> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        dataTransfer1.print();
        System.out.println(dataTransfer1.count());

        //国内外出行偏好
        FlatMapFunction<Tuple6<String, String, String, String, String, String>, Tuple3<String, Integer, Integer>> flatMapFunction10
                = (Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, Integer, Integer>> out) -> {
            Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>();
            String flightCategory = value.f4;
            if (flightCategory.equals("1")) {
                tuple3.setFields(value.f0, 0, 1);
            } else {
                tuple3.setFields(value.f0, 1, 1);
            }
            out.collect(tuple3);
        };


        DataSet<Tuple3<String, Integer, Integer>> dataGroup2 = input_copy.flatMap(flatMapFunction10).returns(new TypeHint<Tuple3<String, Integer, Integer>>() {
            @Override
            public TypeInformation<Tuple3<String, Integer, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).groupBy(0, 1).sum(2);

        dataGroup2.print();
        System.out.println(dataGroup2.count());

        FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>> flatMapFunction11 =
                (Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, String>> out) -> {
                    LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
                    Integer count = value.f2;
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
                .flatMap(flatMapFunction11).returns(new TypeHint<Tuple2<String, String>>() {
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

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction12 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("baiYunId", value.f0);
            map.put("longTengId", "");
            map.put("travelPreference_flightSegment", value.f1);
            map.put("updateTime", new Date());
            out.collect(map);
        };

        DataSet<Map> dataTransfer2 = dataMerge2.flatMap(flatMapFunction12).returns(new TypeHint<Map>() {
            @Override
            public TypeInformation<Map> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        dataTransfer2.print();
        System.out.println(dataTransfer2.count());

        //常用机场
        List<DataSet<Tuple3<String, String, Integer>>> dataGroups = new ArrayList<>();
        //常用起飞机场

        FlatMapFunction<Tuple6<String, String, String, String, String, String>, Tuple3<String, String, Integer>> flatMapFunction13
                = (Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, String, Integer>> out) -> {
            Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
            tuple3.setFields(value.f0, value.f3, 1);
            out.collect(tuple3);
        };

        DataSet<Tuple3<String, String, Integer>> dataGroup3 = input.flatMap(flatMapFunction13).returns(new TypeHint<Tuple3<String, String, Integer>>() {
            @Override
            public TypeInformation<Tuple3<String, String, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).groupBy(0, 1).sum(2);

        dataGroups.add(dataGroup3);
        dataGroup3.print();
        System.out.println(dataGroup3.count());

        //常用降落机场

        FlatMapFunction<Tuple6<String, String, String, String, String, String>, Tuple3<String, String, Integer>> flatMapFunction14
                = (Tuple6<String, String, String, String, String, String> value, Collector<Tuple3<String, String, Integer>> out) -> {
            Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
            tuple3.setFields(value.f0, value.f4, 1);
            out.collect(tuple3);
        };

        DataSet<Tuple3<String, String, Integer>> dataGroup4 = input.flatMap(flatMapFunction14).returns(new TypeHint<Tuple3<String, String, Integer>>() {
            @Override
            public TypeInformation<Tuple3<String, String, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).groupBy(0, 1).sum(2);

        dataGroups.add(dataGroup4);
        dataGroup4.print();
        System.out.println(dataGroup4.count());

        LabelProcessingUtil labelProcessingUtil= new LabelProcessingUtil();
        List<DataSet<Tuple2<String, String>>> dataMerges = labelProcessingUtil.commonAirport(dataGroups);

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction15 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("baiYunId", value.f0);
            map.put("longTengId", "");
            map.put("travelPreference_departAirport", value.f1);
            map.put("updateTime", new Date());
            out.collect(map);
        };

        DataSet<Map> dataTransfer3 = dataMerges.get(0).flatMap(flatMapFunction15).returns(new TypeHint<Map>() {
            @Override
            public TypeInformation<Map> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        dataTransfer3.print();
        System.out.println(dataTransfer3.count());

        FlatMapFunction<Tuple2<String, String>, Map> flatMapFunction16 = (Tuple2<String, String> value, Collector<Map> out) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("baiYunId", value.f0);
            map.put("longTengId", "");
            map.put("travelPreference_arriveAirport", value.f1);
            map.put("updateTime", new Date());
            out.collect(map);
        };

        DataSet<Map> dataTransfer4 = dataMerges.get(1).flatMap(flatMapFunction16).returns(new TypeHint<Map>() {
            @Override
            public TypeInformation<Map> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        dataTransfer4.print();
        System.out.println(dataTransfer4.count());

        DataSet<Map> dataTransfer_flight = dataTransfer0.union(dataTransfer1).union(dataTransfer2).union(dataTransfer3).union(dataTransfer4);
        dataTransfer_flight.print();
        System.out.println(dataTransfer_flight.count());

        if (dataTransfer_flight.count() != 0) {
            if (dataTransfer == null) {
                dataTransfer = dataTransfer_flight;
            } else {
                dataTransfer = dataTransfer.union(dataTransfer_flight);
            }
        }

        if (dataTransfer != null && dataTransfer.count() > 0) {
            dataTransfer.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
            env.execute("travelPreference_travelLabel_by");
            dataTransfer.print();
            System.out.println(dataTransfer.count());
        }

    }

}
