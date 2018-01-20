package com.adatafun.computing.platform.io;

import com.adatafun.computing.platform.util.DataEncapsulationUtil;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * DataSetInputFromMysqlParam.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/6.
 */
public class DataSetInputFromMysqlParam extends BaseDataSetInputFromMysql {

    private String start;
    private String stop;



    public DataSetInputFromMysqlParam(String driver, String url, String username, String password, String sql,
                                      String start, String stop) {
        super(driver, url, username, password, sql);
        this.start = start;
        this.stop = stop;
    }


    public void open() throws Exception {
        //1.加载驱动
        Class.forName(driver);
        //2.创建连接
        connection = DriverManager.getConnection(url, username, password);
        //3.获得执行语句
        ps = connection.prepareStatement(sql);
        ps.setString(1, start);
        ps.setString(2, stop);
    }

}
