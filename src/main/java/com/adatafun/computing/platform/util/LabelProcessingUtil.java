package com.adatafun.computing.platform.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.*;

/**
 * LabelProcessingUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/23.
 */
public class LabelProcessingUtil {

    public DataSet<Map> LabelDataMerge(List<DataSet<Map>> list) {

        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == null) {
                list.remove(list.get(i));
            }
        }

        DataSet<Map> transfer;
        if (!list.isEmpty()) {
            transfer = list.get(0);
            for (int i = 1; i < list.size(); i++) {
                transfer.union(list.get(i));
            }
        } else {
            transfer = null;
        }

        return transfer;

    }

    public Tuple2<String, String> holidayTravel(Map value) {
        DateUtil dateUtil = new DateUtil();
        Tuple2<String, String> tuple2 = new Tuple2<>();

        String loginDate = value.get("createTime").toString();
        Date date = dateUtil.getDate(loginDate);
        Boolean flag = dateUtil.isHoliday(date);

        if (flag) {
            tuple2.setFields(value.get("longTengId").toString(), "节假日出行");
        } else {
            tuple2.setFields(value.get("longTengId").toString(), "非节假日出行");
        }
        return tuple2;
    }

    public Map<String, Object> weekendTravel(Tuple3<String, String, Integer> value) throws ParseException {
        DateUtil dateUtil = new DateUtil();
        Map<String, Object> map = new HashMap<>();
        Integer count = value.getField(2);
        map.put("longTengId", value.getField(0));
        map.put("baiYunId", "");
        if (value.getField(1).equals("非周末出行")) {
            if (count > 20) {
                map.put("travelPreference_weekday", "经常非周末出行");
            } else if (count > 3) {
                map.put("travelPreference_weekday", "偶尔非周末出行");
            } else {
                map.put("travelPreference_weekday", "几乎没有非周末出行");
            }
            map.put("loginNum_weekday", value.getField(2));
        } else {
            if (count > 15) {
                map.put("travelPreference_weekend", "经常周末出行");
            } else if (count > 3) {
                map.put("travelPreference_weekend", "偶尔周末出行");
            } else {
                map.put("travelPreference_weekend", "几乎没有周末出行");
            }
            map.put("loginNum_weekend", value.getField(2));
        }
        map.put("updateTime", dateUtil.setTimeZone(new Date()));
        return map;
    }

    public Tuple2<String, String> travelDemand(Tuple2<String, String> value, Integer remainingTime) {
        Tuple2<String, String> tuple2 = new Tuple2<>();
        if (remainingTime < 24*60) {
            tuple2.setFields(value.f0, "24小时内有出行需求");
        } else if (remainingTime < 48*60) {
            tuple2.setFields(value.f0, "48小时内有出行需求");
        } else if (remainingTime < 7*24*60) {
            tuple2.setFields(value.f0, "7天内有出行需求");
        } else if (remainingTime < 30*24*60) {
            tuple2.setFields(value.f0, "30天内有出行需求");
        } else {
            tuple2.setFields(value.f0, "非近期出行");
        }
        return tuple2;
    }

    public Tuple2<String, String> flightPreference(Tuple3<String, Integer, Integer> value) {
        Tuple2<String, String> tuple2 = new Tuple2<>();
        Integer type = value.f1;
        switch (type) {
            case 0:
                tuple2.setFields(value.f0, "红眼航班");
                break;
            case 1:
                tuple2.setFields(value.f0, "早上班机");
                break;
            case 2:
                tuple2.setFields(value.f0, "中午班机");
                break;
            case 3:
                tuple2.setFields(value.f0, "下午班机");
                break;
            case 4:
                tuple2.setFields(value.f0, "晚上航班");
                break;
        }
        return tuple2;
    }

    public Tuple2<String, String> flightDistance(Tuple3<String, Integer, Integer> value) {
        Tuple2<String, String> tuple2 = new Tuple2<>();
        Integer type = value.f1;
        switch (type) {
            case 0:
                tuple2.setFields(value.f0, "长途航班");
                break;
            case 1:
                tuple2.setFields(value.f0, "中途航班");
                break;
            case 2:
                tuple2.setFields(value.f0, "短途航班");
                break;
        }
        return tuple2;
    }

    public Tuple2<String, String> flightSegment(Tuple3<String, Integer, Integer> value) {
        Tuple2<String, String> tuple2 = new Tuple2<>();
        Integer type = value.f1;
        switch (type) {
            case 0:
                tuple2.setFields(value.f0, "国内航段偏好");
                break;
            case 1:
                tuple2.setFields(value.f0, "国际航段偏好");
                break;
        }
        return tuple2;
    }

    public List<DataSet<Tuple2<String, String>>> commonAirport(List<DataSet<Tuple3<String, String, Integer>>> dataGroups)
    throws Exception {
        List<DataSet<Tuple2<String, String>>> dataMerges = new ArrayList<>();

        for (DataSet<Tuple3<String, String, Integer>> dataGroup : dataGroups) {
            DataSet<Tuple2<String, String>> dataMerge = dataGroup.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
                public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException {

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
        return dataMerges;
    }

    public DataSet<Tuple2<String, String>> oftenUsedAirport(DataSet<Tuple3<String, String, Integer>> dataGroup)
            throws Exception {

        DataSet<Tuple2<String, String>> dataMerge = dataGroup.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
            public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws ParseException {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                Integer count = value.f2;
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

        dataMerge.print();
        System.out.println(dataMerge.count());
        return dataMerge;
    }

}
