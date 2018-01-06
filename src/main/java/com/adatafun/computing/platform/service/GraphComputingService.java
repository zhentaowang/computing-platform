package com.adatafun.computing.platform.service;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * XXX.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/29.
 */
public class GraphComputingService {

    public void UserIdMapping () {
        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();
        Vertex<String, Long> vertex = new Vertex<>();
        vertex.setId("0001");
        vertex.setValue((long) 500);
//        DataSet<Vertex<String, Long>> vertices;
//        DataSet<Edge<Double, Long>> edges;
//        Graph<String, Long, Double> graph = Graph.fromDataSet(vertices, edges, env);
    }

}
