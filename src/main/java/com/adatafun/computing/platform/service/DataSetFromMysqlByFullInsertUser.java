package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetBulkOutputToElasticSearchFromMap;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlNoParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearch;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchFromMap;
import com.adatafun.computing.platform.model.PlatformUser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
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
 * DataSetFromMysqlByFullInsertUser.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/21.
 */
public class DataSetFromMysqlByFullInsertUser {

    public static void main(String[] args) throws Exception {

        MysqlConf mysqlConf = new MysqlConf();
        String sql1 = "select id as longTengId, phone_str as phoneNum, alipay_user_id as alipayId, email_str as email from tb_user;";
        DataSetInputFromMysqlNoParam dataSetInput_lt = new DataSetInputFromMysqlNoParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql1);
        List<Tuple4<String, String, String, String>> userList_lt = dataSetInput_lt.dataEncapsulationTuple4ByPartUpdate();
        System.out.println(userList_lt.size() + "lt connect successful");

        Integer split = 300000;
        Integer count = userList_lt.size();
        for (int i = 0; i < count/split+1; i++) {
            Integer toIndex = count > (i+1)*split ? (i+1)*split : count;
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSet<Tuple4<String, String, String, String>> input_lt = env.fromCollection(userList_lt.subList(i*split, toIndex));

            DataSet<Map<String, Object>> dataTransfer_lt = input_lt.flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, Map<String, Object>>() {
                public void flatMap(Tuple4<String, String, String, String> value, Collector<Map<String, Object>> out) throws ParseException {

                    Map<String, Object> map = new HashMap<>();
                    map.put("longTengId", value.f0);
                    map.put("baiYunId", "");
                    map.put("phoneNum", value.f1);
                    map.put("alipayId", value.f2);
                    map.put("email", value.f3);
                    map.put("createTime", new Date());
                    map.put("updateTime", new Date());
                    out.collect(map);

                }
            });

            dataTransfer_lt.output(new DataSetOutputToElasticSearchFromMap("dmp-user", "dmp-user"));
            env.execute();
            Thread.sleep(1000*60);
//            dataTransfer_lt.print();
//            System.out.println(dataTransfer_lt.count());
        }

    }

}
