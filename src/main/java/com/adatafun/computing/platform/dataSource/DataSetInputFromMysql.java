package com.adatafun.computing.platform.dataSource;

import com.adatafun.computing.platform.indexMap.PlatformUser;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public List<PlatformUser> run() throws Exception {
        try {
            //4.执行查询，封装数据
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<PlatformUser> userList = new ArrayList<>();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while (resultSet.next()) {
                PlatformUser value = new PlatformUser();
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = resultSetMetaData.getColumnLabel(i);
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue != null) {
                        map.put(columnName, columnValue);
                    } else if (!columnName.equals("updateTime") && !columnName.equals("createTime")) {
                        map.put(columnName, "null flag");
                    } else {
                        map.put(columnName, "1000-01-01 00:00:00");
                    }
                }
                if (map.containsKey("phoneNum")) {
                    value.setPhoneNum(map.get("phoneNum").toString());
                }
                if (map.containsKey("deviceNum")) {
                    value.setDeviceNum(map.get("deviceNum").toString());
                }
                if (map.containsKey("idNum")) {
                    value.setIdNum(map.get("idNum").toString());
                }
                if (map.containsKey("passportNum")) {
                    value.setPassportNum(map.get("passportNum").toString());
                }
                if (map.containsKey("longTengId")) {
                    value.setLongTengId(map.get("longTengId").toString());
                }
                if (map.containsKey("baiYunId")) {
                    value.setBaiYunId(map.get("baiYunId").toString());
                }
                if (map.containsKey("alipayId")) {
                    value.setAlipayId(map.get("alipayId").toString());
                }
                if (map.containsKey("email")) {
                    value.setEmail(map.get("email").toString());
                }
                if (map.containsKey("updateTime")) {
                    java.util.Date updateTime = simpleDateFormat.parse(map.get("updateTime").toString());
                    value.setUpdateTime(updateTime);
                }
                if (map.containsKey("createTime")) {
                    java.util.Date createTime = simpleDateFormat.parse(map.get("createTime").toString());
                    value.setCreateTime(createTime);
                }
                userList.add(value);
            }
            return userList;
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
}
