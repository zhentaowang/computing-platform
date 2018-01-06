package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.dataSource.DataSetInputFromMysql;
import com.adatafun.computing.platform.dataSource.DataSetOutputToElasticSearch;
import com.adatafun.computing.platform.indexMap.PlatformUser;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

/**
 * XXX.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/29.
 */
public class GraphComputingService {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String driver = "com.mysql.jdbc.Driver";

        String url1 = "jdbc:mysql://localhost:3306/demo?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
        String username1 = "root";
        String password1 = "w19890528";
        String sql1 = "select id as longTengId, mobileNo as phoneNum, cardNo as idNum, updateTime, createTime from student;";

        String url2 = "jdbc:mysql://localhost:3306/demo00?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
        String username2 = "root";
        String password2 = "w19890528";
        String sql2 = "select id as baiYunId, phone as phoneNum, card as idNum, updateTime, createTime from user;";

        DataSetInputFromMysql dataSetInput1 = new DataSetInputFromMysql(driver, url1, username1, password1, sql1);
        dataSetInput1.open();
        List<PlatformUser> userList1 = dataSetInput1.run();
        dataSetInput1.close();
        DataSetInputFromMysql dataSetInput2 = new DataSetInputFromMysql(driver, url2, username2, password2, sql2);
        dataSetInput2.open();
        List<PlatformUser> userList2 = dataSetInput2.run();
        dataSetInput2.close();

        DataSet<PlatformUser> input1 = env.fromCollection(userList1);
        DataSet<PlatformUser> input2 = env.fromCollection(userList2);
        DataSet<PlatformUser> input = input1.join(input2).where("phoneNum").equalTo("phoneNum")
                .with(new JoinFunction<PlatformUser, PlatformUser, PlatformUser>() {
            public PlatformUser join(PlatformUser v1, PlatformUser v2) {
                // NOTE:
                // - v2 might be null for leftOuterJoin
                // - v1 might be null for rightOuterJoin
                // - v1 OR v2 might be null for fullOuterJoin
                Boolean isLatest = v1.getCreateTime().compareTo(v2.getCreateTime()) == 1;
                String phoneNum = getLatestData(v1.getPhoneNum(),v2.getPhoneNum(), isLatest);
                String idNum = getLatestData(v1.getIdNum(),v2.getIdNum(), isLatest);
                return new PlatformUser(v1.getLongTengId(), phoneNum, v2.getBaiYunId(), idNum);
            }
        });
        input.print();
        input.output(new DataSetOutputToElasticSearch("dmp-user", "dmp-user"));
        env.execute();
    }
    
    private static String getLatestData(String n1, String n2, Boolean n3) {
        String n4;
        if (!n1.equals(n2)) {
            if (!n1.equals("null flag") && !n2.equals("null flag")) {
                n4 = n3 ? n1 : n2;
            } else {
                n4 = n2.equals("null flag") ? n1 : n2;
            }
        } else {
            n4 = n1;
        }
        return n4;
    }

}
