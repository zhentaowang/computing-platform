package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.io.DataSetInputFromMysql;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearch;
import com.adatafun.computing.platform.model.PlatformUser;
import com.adatafun.computing.platform.util.DataEncapsulationUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * GraphComputingFromMysqlService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/9.
 */
public class GraphComputingFromMysqlService {

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
        List<PlatformUser> studentList = dataSetInput1.readFromMysql();

        DataSetInputFromMysql dataSetInput2 = new DataSetInputFromMysql(driver, url2, username2, password2, sql2);
        List<PlatformUser> userList = dataSetInput2.readFromMysql();

        List<Vertex<Long, PlatformUser>> studentVertices = new ArrayList<>();
        for (int i = 0; i < studentList.size(); i++) {
            studentVertices.add(new Vertex<>(i+1L, studentList.get(i)));
        }
        List<Edge<Long, Double>> studentEdges = new ArrayList<>();
        studentEdges.add(new Edge<>(1L, 2L, 0.0));

        List<Tuple2<Long, PlatformUser>> userVertices = new ArrayList<>();
        for (int i = 0; i < userList.size(); i++) {
            userVertices.add(new Tuple2<>(i+1L, userList.get(i)));
        }

        Graph<Long, PlatformUser, Double> studentGraph = Graph.fromDataSet(env.fromCollection(studentVertices),
                env.fromCollection(studentEdges), env);

        Graph<Long, PlatformUser, Double> platformUserGraph = studentGraph.joinWithVertices(env.fromCollection(userVertices),
                new VertexJoinFunction<PlatformUser, PlatformUser>() {
            @Override
            public PlatformUser vertexJoin(PlatformUser u1, PlatformUser u2) throws Exception {
                Boolean isMatched = u1.getPhoneNum().equals(u2.getPhoneNum()) || u1.getDeviceNum().equals(u2.getDeviceNum())
                        || u1.getIdNum().equals(u2.getIdNum()) || u1.getPassportNum().equals(u2.getPassportNum())
                        || u1.getEmail().equals(u2.getEmail());
                if (isMatched) {
                    if (u1.getCreateTime().compareTo(u2.getCreateTime()) == 1) {
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
                    }
                }
                return u1;
            }
        });
        platformUserGraph.getVertices().print();
        List<Vertex<Long, PlatformUser>> vertexList = platformUserGraph.getVertices().collect();
        List<PlatformUser> platformUserList = new ArrayList<>();
        for (Vertex<Long, PlatformUser> vertex : vertexList) {
            platformUserList.add(vertex.getValue());
        }
        env.fromCollection(platformUserList).output(new DataSetOutputToElasticSearch("dmp-user", "dmp-user"));
        env.execute();
    }

}
