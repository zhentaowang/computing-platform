package com.adatafun.computing.platform.io;

import com.adatafun.computing.platform.model.PlatformUser;
import com.adatafun.computing.platform.util.DataEncapsulationUtil;

import java.sql.*;
import java.util.List;

/**
 * DataSetInputFromMysql.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/6.
 */
public class DataSetInputFromMysql {

    private PreparedStatement ps;
    private Connection connection;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String sql;

    public DataSetInputFromMysql(String driver, String url, String username, String password, String sql) {
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

    public ResultSet run() throws Exception {
        try {
            //4.执行查询
            return ps.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void close() throws Exception {
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

}
