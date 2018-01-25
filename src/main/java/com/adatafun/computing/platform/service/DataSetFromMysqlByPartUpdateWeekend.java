package com.adatafun.computing.platform.service;

/**
 * DataSetFromMysqlByPartUpdateWeekend.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/18.
 */
public class DataSetFromMysqlByPartUpdateWeekend {

    public static void main(String[] args) throws Exception {

//        DateUtil holidayUtil1 = new DateUtil();
//        Map<String, String> map = holidayUtil1.getDateInterval(Calendar.DAY_OF_MONTH, -90);//把日期往前减90天
//        String startTime = map.get("startTime");
//        String stopTime = map.get("stopTime");
//
//        MysqlConf mysqlConf = new MysqlConf();
//        DataSetInputFromMysqlParam dataSetInput = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl3(),
//                mysqlConf.getUsername3(), mysqlConf.getPassword3(), mysqlConf.getSql3(), startTime, stopTime);
//        List<Map> userList = dataSetInput.readFromMysqlByPartUpdate();
//        System.out.println(userList.size() + "lt connect successful");
//
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSet<Map> input = env.fromCollection(userList);
//
//        DataSet<Tuple3<String, String, Integer>> dataGroup = input.flatMap(new FlatMapFunction<Map, Tuple3<String, String, Integer>>() {
//            public void flatMap(Map value, Collector<Tuple3<String, String, Integer>> out) throws ParseException{
//
//                DateUtil holidayUtil = new DateUtil();
//                Tuple3<String, String, Integer> tuple3 = new Tuple3<>();
//
//                String loginDate = value.get("createTime").toString();
//                Date date = holidayUtil.getDate(loginDate);
//
//                if (holidayUtil.isWeekend(date)) {
//                    tuple3.setFields(value.get("longTengId").toString(), "周末出行", 1);
//                } else {
//                    tuple3.setFields(value.get("longTengId").toString(), "非周末出行", 1);
//                }
//
//                out.collect(tuple3);
//
//            }
//        }).groupBy(0, 1).sum(2);
//
//        dataGroup.print();
//        System.out.println(dataGroup.count());
//
//        DataSet<Map> dataCount = dataGroup.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Map>() {
//            public void flatMap(Tuple3<String, String, Integer> value, Collector<Map> out) throws ParseException{
//                LabelProcessingUtil labelProcessingUtil = new LabelProcessingUtil();
//                out.collect(labelProcessingUtil.weekendTravel(value));
//            }
//        });
//
//        dataCount.output(new DataSetOutputToElasticSearchByPartUpdate("dmp-user", "dmp-user"));
//        dataCount.print();
//        System.out.println(dataCount.count());

    }

}
