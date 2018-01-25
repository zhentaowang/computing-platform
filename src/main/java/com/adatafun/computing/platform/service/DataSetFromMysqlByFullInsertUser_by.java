package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchFromMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataSetFromMysqlByFullInsertUser_by.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/22.
 */
public class DataSetFromMysqlByFullInsertUser_by {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        String sql2 = "select open_id as baiYunId, client_phone as phoneNum, client_identity_card as idNum," +
                " client_passport_number as passportNum from client_info;";
        DataSetInputFromMysqlNoParam dataSetInput_by = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), sql2);
        List<Tuple4<String, String, String, String>> userList_by = dataSetInput_by.dataEncapsulationTuple4ByPullInsert();
        System.out.println(userList_by.size() + "by connect successful");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<String, String, String, String>> input_by = env.fromCollection(userList_by);

        DataSet<Map<String, Object>> dataTransfer_by = input_by.flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, Map<String, Object>>() {
            public void flatMap(Tuple4<String, String, String, String> value, Collector<Map<String, Object>> out) throws ParseException {

                Map<String, Object> map = new HashMap<>();
                map.put("longTengId", "");
                map.put("baiYunId", value.f0);
                map.put("phoneNum", value.f1);
                map.put("idNum", value.f2);
                map.put("passportNum", value.f3);
                map.put("createTime", new Date());
                map.put("updateTime", new Date());
                out.collect(map);

            }
        });

        dataTransfer_by.output(new DataSetOutputToElasticSearchFromMap("dmp-user", "dmp-user"));
        dataTransfer_by.print();
        System.out.println(dataTransfer_by.count());
    }

}
