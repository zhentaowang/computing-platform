package com.adatafun.computing.platform.io;

import com.adatafun.computing.platform.model.PlatformUser;
import com.adatafun.computing.platform.util.DataEncapsulationUtil;
import org.apache.flink.api.java.tuple.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/**
 * BaseDataSetInputFromMysql.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/18.
 */
public class BaseDataSetInputFromMysql {

    PreparedStatement ps;
    Connection connection;
    String driver;
    String url;
    String username;
    String password;
    String sql;

    BaseDataSetInputFromMysql(String driver, String url, String username, String password, String sql) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.sql = sql;
    }

    public void open() throws Exception {
        //1.加载驱动
        Class.forName(driver);
        //2.创建连接
        connection = DriverManager.getConnection(url, username, password);
        //3.获得执行语句
        ps = connection.prepareStatement(sql);
    }

    private ResultSet run() throws Exception {
        try {
            //4.执行查询
            return ps.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void close() throws Exception {
        //5.关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    public List<PlatformUser> readFromMysql() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<PlatformUser> userList = dataEncapsulationUtil.dataEncapsulation(resultSet1);
        close();
        return userList;
    }

    public List<Map> readFromMysqlByPartUpdate() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Map> userList = dataEncapsulationUtil.dataEncapsulationByPartUpdate(resultSet1);
        close();
        return userList;
    }

    public List<Tuple2<Long, String>> dataEncapsulationTuple2ByPartUpdate() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple2<Long, String>> userList = dataEncapsulationUtil.dataEncapsulationTuple2ByPartUpdate(resultSet1);
        close();
        return userList;
    }

    public List<Tuple2<String, String>> dataEncapsulationTuple2ByString() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple2<String, String>> userList = dataEncapsulationUtil.dataEncapsulationTuple2ByString(resultSet1);
        close();
        return userList;
    }

    public List<Tuple2<String, String>> dataEncapsulationTuple2ByAES() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple2<String, String>> userList = dataEncapsulationUtil.dataEncapsulationTuple2ByAES(resultSet1);
        close();
        return userList;
    }

    public List<Tuple2<String, String>> dataEncapsulationTuple2ByPartner() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple2<String, String>> userList = dataEncapsulationUtil.dataEncapsulationTuple2ByPartner(resultSet1);
        close();
        return userList;
    }

    public List<Tuple6<String, String, String, String, String, String>> dataEncapsulationTuple6ByString() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple6<String, String, String, String, String, String>> userList = dataEncapsulationUtil
                .dataEncapsulationTuple6ByString(resultSet1);
        close();
        return userList;
    }

    public List<Tuple3<String, String, String>> dataEncapsulationTuple3ByPartUpdate() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple3<String, String, String>> userList = dataEncapsulationUtil.dataEncapsulationTuple3ByPartUpdate(resultSet1);
        close();
        return userList;
    }

    public List<Tuple4<String, String, String, String>> dataEncapsulationTuple4ByPartUpdate() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple4<String, String, String, String>> userList = dataEncapsulationUtil.dataEncapsulationTuple4ByPartUpdate(resultSet1);
        close();
        return userList;
    }

    public List<Tuple4<String, String, String, String>> dataEncapsulationTuple4ByPullInsert() throws Exception {
        open();
        ResultSet resultSet1 = run();
        DataEncapsulationUtil dataEncapsulationUtil = new DataEncapsulationUtil();
        List<Tuple4<String, String, String, String>> userList = dataEncapsulationUtil.dataEncapsulationTuple4ByPullInsert(resultSet1);
        close();
        return userList;
    }

}
