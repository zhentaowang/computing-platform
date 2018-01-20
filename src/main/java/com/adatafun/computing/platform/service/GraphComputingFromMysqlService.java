package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearch;
import com.adatafun.computing.platform.model.PlatformUser;
import com.adatafun.computing.platform.util.GraphOperatorUtil;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * GraphComputingFromMysqlService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/9.
 */
public class GraphComputingFromMysqlService {

    public static void main(String[] args) throws Exception {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//HH:24小时制，hh:12小时制

        String minDate_long = "2012-10-26 00:00:00";
        Calendar calendar_long = new GregorianCalendar();
        calendar_long.setTime(simpleDateFormat.parse(minDate_long));
        String startTime_long;
        String stopTime_long = simpleDateFormat.format(calendar_long.getTime());

        MysqlConf mysqlConf = new MysqlConf();
        GraphOperatorUtil graphOperatorUtil = new GraphOperatorUtil();

        for (int m = 0; m < 64; m++) {

            startTime_long = stopTime_long;
            calendar_long.add(Calendar.MONTH, 1);//把日期往后增加一个月.正数往后推,负数往前移动
            stopTime_long = simpleDateFormat.format(calendar_long.getTime());

            String minDate_bai = "2016-12-13 00:00:00";
            Calendar calendar_bai = new GregorianCalendar();
            calendar_bai.setTime(simpleDateFormat.parse(minDate_bai));
            String startTime_bai;
            String stopTime_bai = simpleDateFormat.format(calendar_bai.getTime());

            DataSetInputFromMysqlParam dataSetInput1 = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                    mysqlConf.getUsername1(), mysqlConf.getPassword1(), mysqlConf.getSql1(), startTime_long, stopTime_long);
            List<PlatformUser> studentList = dataSetInput1.readFromMysql();
            if (studentList.size() == 0) {
                continue;
            }
            System.out.println("lt connect successful");

            for (int n = 0; n < 14; n++) {

                startTime_bai = stopTime_bai;
                calendar_bai.add(Calendar.MONTH, 1);//把日期往后增加一个月.正数往后推,负数往前移动
                stopTime_bai = simpleDateFormat.format(calendar_bai.getTime());

                DataSetInputFromMysqlParam dataSetInput2 = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                        mysqlConf.getUsername2(), mysqlConf.getPassword2(), mysqlConf.getSql2(), startTime_bai, stopTime_bai);
                List<PlatformUser> userList = dataSetInput2.readFromMysql();
                if (userList.size() == 0) {
                    continue;
                }
                System.out.println("by connect successful");

                List<Vertex<Long, PlatformUser>> studentVertices = new ArrayList<>();
                for (int i = 0; i < studentList.size(); i++) {
                    studentVertices.add(new Vertex<>(i+1L, studentList.get(i)));
                }
                List<Edge<Long, Double>> studentEdges = new ArrayList<>();
                studentEdges.add(new Edge<>(1L, 2L, 0.0));

                List<Tuple2<Long, PlatformUser>> userVertices = new ArrayList<>();
                for (int j = 0; j < userList.size(); j++) {
                    userVertices.add(new Tuple2<>(j+1L, userList.get(j)));
                }

                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                Graph<Long, PlatformUser, Double> studentGraph = Graph.fromDataSet(env.fromCollection(studentVertices),
                        env.fromCollection(studentEdges), env);

                Graph<Long, PlatformUser, Double> platformUserGraph = studentGraph.joinWithVertices(env.fromCollection(userVertices),
                        new VertexJoinFunction<PlatformUser, PlatformUser>() {
                            @Override
                            public PlatformUser vertexJoin(PlatformUser u1, PlatformUser u2) throws Exception {
                                Boolean isMatched = (!u1.getPhoneNum().equals("") && u1.getPhoneNum().equals(u2.getPhoneNum()))
                                        || (!u1.getDeviceNum().equals("") && u1.getDeviceNum().equals(u2.getDeviceNum()))
                                        || (!u1.getIdNum().equals("") && u1.getIdNum().equals(u2.getIdNum()))
                                        || (!u1.getPassportNum().equals("") && u1.getPassportNum().equals(u2.getPassportNum()))
                                        || (!u1.getEmail().equals("") && u1.getEmail().equals(u2.getEmail()));
                                if (isMatched) {
                                    if (u1.getPhoneNum().equals("") && !u2.getPhoneNum().equals("")) {
                                        u1.setPhoneNum(u2.getPhoneNum());
                                    }
                                    if (u1.getDeviceNum().equals("") && !u2.getDeviceNum().equals("")) {
                                        u1.setDeviceNum(u2.getDeviceNum());
                                    }
                                    if (u1.getIdNum().equals("") && !u2.getIdNum().equals("")) {
                                        u1.setIdNum(u2.getIdNum());
                                    }
                                    if (u1.getPassportNum().equals("") && !u2.getPassportNum().equals("")) {
                                        u1.setPassportNum(u2.getPassportNum());
                                    }
                                    if (u1.getEmail().equals("") && !u2.getEmail().equals("")) {
                                        u1.setEmail(u2.getEmail());
                                    }
                                    u1.setBaiYunId(u2.getBaiYunId());
                                }
                                return u1;
                            }
                        });
                List<Vertex<Long, PlatformUser>> vertexList = platformUserGraph.getVertices().collect();
                List<PlatformUser> platformUserList = new ArrayList<>();

                if (n == 0) {
                    for (Vertex<Long, PlatformUser> vertex : vertexList) {
                        platformUserList.add(vertex.getValue());
                    }
                } else {
                    for (Vertex<Long, PlatformUser> vertex : vertexList) {
                        PlatformUser platformUser = vertex.getValue();
                        if (!platformUser.getLongTengId().equals("") && !platformUser.getBaiYunId().equals("")) {
                            platformUserList.add(vertex.getValue());
                        }
                    }
                }

                if (m == 0) {
                    platformUserList = graphOperatorUtil.vertexRemove(userList, platformUserList);
                }

                if (platformUserList.size() != 0) {
                    env.fromCollection(platformUserList).output(new DataSetOutputToElasticSearch("dmp-user", "dmp-user"));
                    env.execute();
                    Thread.sleep(1000);
                }

            }
        }
    }

}
