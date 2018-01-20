package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByDay;
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
 * GraphComputingFromMysqlByDayService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/16.
 */
public class GraphComputingFromMysqlByDayService {

    public static void main(String[] args) throws Exception {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//HH:24小时制，hh:12小时制

//        String minDate = "2012-10-26 00:00:00";
        String minDate = simpleDateFormat.format(new Date());
        System.out.println(minDate);
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(simpleDateFormat.parse(minDate));
        String stopTime = simpleDateFormat.format(calendar.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, -1);//把日期往前减少一天.正数往后推,负数往前移动
        String startTime = simpleDateFormat.format(calendar.getTime());

        MysqlConf mysqlConf = new MysqlConf();
        GraphOperatorUtil graphOperatorUtil = new GraphOperatorUtil();

        DataSetInputFromMysqlParam dataSetInput1 = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), mysqlConf.getSql1(), startTime, stopTime);
        List<PlatformUser> studentListByDay = dataSetInput1.readFromMysql();
        System.out.println(studentListByDay.size() + "test lt connect successful");

        DataSetInputFromMysqlParam dataSetInput2 = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), mysqlConf.getSql2(), startTime, stopTime);
        List<PlatformUser> userListByDay = dataSetInput2.readFromMysql();
        System.out.println(userListByDay.size() + "test by connect successful");

        List<Vertex<Long, PlatformUser>> studentVertices = new ArrayList<>();
        for (int i = 0; i < studentListByDay.size(); i++) {
            studentVertices.add(new Vertex<>(i+1L, studentListByDay.get(i)));
        }
        List<Edge<Long, Double>> studentEdges = new ArrayList<>();
        studentEdges.add(new Edge<>(1L, 2L, 0.0));

        List<Tuple2<Long, PlatformUser>> userVertices = new ArrayList<>();
        for (int j = 0; j < userListByDay.size(); j++) {
            userVertices.add(new Tuple2<>(j+1L, userListByDay.get(j)));
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, PlatformUser, Double> studentGraphByDay = Graph.fromDataSet(env.fromCollection(studentVertices),
                env.fromCollection(studentEdges), env);

        Graph<Long, PlatformUser, Double> platformUserGraphByDay = studentGraphByDay.joinWithVertices(env.fromCollection(userVertices),
                new VertexJoinFunction<PlatformUser, PlatformUser>() {
                    @Override
                    public PlatformUser vertexJoin(PlatformUser u1, PlatformUser u2) throws Exception {
                        String phoneNum_lt = u1.getPhoneNum();
                        String deviceNum_lt = u1.getDeviceNum();
                        String idNum_lt = u1.getIdNum();
                        String passportNum_lt = u1.getPassportNum();
                        String email_lt = u1.getEmail();
                        String phoneNum_by = u2.getPhoneNum();
                        String deviceNum_by = u2.getDeviceNum();
                        String idNum_by = u2.getIdNum();
                        String passportNum_by = u2.getPassportNum();
                        String email_by = u2.getEmail();
                        Boolean isMatched = (!phoneNum_lt.equals("") && phoneNum_lt.equals(phoneNum_by))
                                || (!deviceNum_lt.equals("") && deviceNum_lt.equals(deviceNum_by))
                                || (!idNum_lt.equals("") && idNum_lt.equals(idNum_by))
                                || (!passportNum_lt.equals("") && passportNum_lt.equals(passportNum_by))
                                || (!email_lt.equals("") && email_lt.equals(email_by));
                        if (isMatched) {
                            if (phoneNum_lt.equals("") && !phoneNum_by.equals("")) {
                                u1.setPhoneNum(phoneNum_by);
                            }
                            if (deviceNum_lt.equals("") && !deviceNum_by.equals("")) {
                                u1.setDeviceNum(deviceNum_by);
                            }
                            if (idNum_lt.equals("") && !idNum_by.equals("")) {
                                u1.setIdNum(idNum_by);
                            }
                            if (passportNum_lt.equals("") && !passportNum_by.equals("")) {
                                u1.setPassportNum(passportNum_by);
                            }
                            if (email_lt.equals("") && !email_by.equals("")) {
                                u1.setEmail(email_by);
                            }
                            u1.setBaiYunId(u2.getBaiYunId());
                        }
                        return u1;
                    }
                });
        List<Vertex<Long, PlatformUser>> vertexList = platformUserGraphByDay.getVertices().collect();
        List<PlatformUser> platformUserList = new ArrayList<>();
        for (Vertex<Long, PlatformUser> vertex : vertexList) {
            platformUserList.add(vertex.getValue());
        }

        platformUserList = graphOperatorUtil.vertexRemove(userListByDay, platformUserList);

        env.fromCollection(platformUserList).output(new DataSetOutputToElasticSearchByDay("dmp-user", "dmp-user"));
        env.execute();
        Thread.sleep(1000);
    }

}
