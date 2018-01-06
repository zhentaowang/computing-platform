package com.adatafun.computing.platform.kafka;

import com.adatafun.computing.platform.indexMap.UnionUser;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * MysqlSink.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/22.
 */
public class MysqlSink extends RichSinkFunction<UnionUser> {
    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    private String username = "root";
    private String passwd = "w19890528";
    private String driverName = "com.mysql.jdbc.Driver";
    private String dburl = "jdbc:mysql://localhost:3306/user_id_mapping?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";

    @Override
    public void invoke(UnionUser value) throws Exception{
        Class.forName(driverName);
        connection = DriverManager.getConnection(dburl,username,passwd);
        String sql = "insert into id_mapping(name,sex,phone,card,address) values (?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.getName());
        preparedStatement.setString(2, value.getSex());
        preparedStatement.setString(3, value.getPhone());
        preparedStatement.setString(4, value.getCard());
        preparedStatement.setString(5, value.getAddress());
        preparedStatement.executeUpdate();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
    }

}
