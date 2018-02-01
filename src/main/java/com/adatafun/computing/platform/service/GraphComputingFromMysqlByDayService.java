package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByDay;
import com.adatafun.computing.platform.model.PlatformUser;
import com.adatafun.computing.platform.util.DateUtil;
import com.adatafun.computing.platform.util.GraphOperatorUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;

import java.util.*;

/**
 * GraphComputingFromMysqlByDayService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/16.
 */
public class GraphComputingFromMysqlByDayService {

    public static void main(String[] args) throws Exception {

        DateUtil dateUtil = new DateUtil();
        Map<String, String> map = dateUtil.getDateInterval(Calendar.DAY_OF_MONTH, -1);//把日期往前减少一天
        String startTime = map.get("startTime");
        String stopTime = map.get("stopTime");

        MysqlConf mysqlConf = new MysqlConf();
        GraphOperatorUtil graphOperatorUtil = new GraphOperatorUtil();

        DataSetInputFromMysqlParam dataSetInput_lt = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                mysqlConf.getUsername1(), mysqlConf.getPassword1(), mysqlConf.getSql1(), startTime, stopTime);
        List<PlatformUser> userListByDay_lt = dataSetInput_lt.readFromMysql();
        System.out.println(userListByDay_lt.size() + " lt connect successful");

        DataSetInputFromMysqlParam dataSetInput_by = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl2(),
                mysqlConf.getUsername2(), mysqlConf.getPassword2(), mysqlConf.getSql2(), startTime, stopTime);
        List<PlatformUser> userListByDay_by = dataSetInput_by.readFromMysql();
        System.out.println(userListByDay_by.size() + " by connect successful");

        List<Vertex<Long, PlatformUser>> userVertices_lt = new ArrayList<>();
        for (int i = 0; i < userListByDay_lt.size(); i++) {
            userVertices_lt.add(new Vertex<>(i+1L, userListByDay_lt.get(i)));
        }
        List<Edge<Long, Double>> studentEdges_lt = new ArrayList<>();
        studentEdges_lt.add(new Edge<>(1L, 2L, 0.0));

        List<Tuple2<Long, PlatformUser>> userVertices_by = new ArrayList<>();
        for (int j = 0; j < userListByDay_by.size(); j++) {
            userVertices_by.add(new Tuple2<>(j+1L, userListByDay_by.get(j)));
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, PlatformUser, Double> userGraphByDay_lt = Graph.fromDataSet(env.fromCollection(userVertices_lt),
                env.fromCollection(studentEdges_lt), env);

        VertexJoinFunction<PlatformUser, PlatformUser> vertexJoinFunction = (PlatformUser u1, PlatformUser u2) -> {
            GraphOperatorUtil graphOperatorUtil1 = new GraphOperatorUtil();
            return graphOperatorUtil1.vertexMerge(u1, u2);
        };

        Graph<Long, PlatformUser, Double> platformUserGraphByDay = userGraphByDay_lt
                .joinWithVertices(env.fromCollection(userVertices_by), vertexJoinFunction);

        List<Vertex<Long, PlatformUser>> vertexList = platformUserGraphByDay.getVertices().collect();
        List<PlatformUser> platformUserList = new ArrayList<>();
        for (Vertex<Long, PlatformUser> vertex : vertexList) {
            platformUserList.add(vertex.getValue());
        }

        platformUserList = graphOperatorUtil.vertexRemove(userListByDay_by, platformUserList);
        DataSet<PlatformUser> result = env.fromCollection(platformUserList);
        result.output(new DataSetOutputToElasticSearchByDay("dmp-user", "dmp-user"));
        result.print();
        System.out.println(result.count());
    }

}
