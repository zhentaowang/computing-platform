package com.adatafun.computing.platform.dataSource;

import com.adatafun.computing.platform.indexMap.PlatformUser;
import com.adatafun.computing.platform.indexMap.UnionUser;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * DataStreamInputFromMysql.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/26.
 */
public class DataStreamInputFromMysql extends RichSourceFunction<PlatformUser> {
    private PreparedStatement ps;
    private Connection connection;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String sql;

    public DataStreamInputFromMysql(String driver, String url, String username, String password, String sql) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.sql = sql;
    }

    /**
     * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //1.加载驱动
        Class.forName(driver);
        //2.创建连接
        connection = DriverManager.getConnection(url, username, password);
        //3.获得执行语句
        ps = connection.prepareStatement(sql);
    }

    /**
     * 二、DataStream调用一次run()方法用来获取数据
     */
    @Override
    public void run(SourceContext<PlatformUser> sourceContext) throws Exception {
        try {
            //4.执行查询，封装数据
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                PlatformUser value = new PlatformUser();
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object o = resultSet.getObject(i);
                    if (o != null) {
                        map.put(resultSetMetaData.getColumnLabel(i), o);
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
                sourceContext.collect(value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {

    }

    /**
     * 三、 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     */
    @Override
    public void close() throws Exception {
        //5.关闭连接和释放资源
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}

