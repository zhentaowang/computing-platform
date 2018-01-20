package com.adatafun.computing.platform.conf;

/**
 * MysqlConf.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/17.
 */
public class MysqlConf {

    private String driver = "com.mysql.cj.jdbc.Driver";

//        String url1 = "jdbc:mysql://localhost:3306/demo?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
//        String username1 = "root";
//        String password1 = "w19890528";
//        String sql1 = "select id as longTengId, mobileNo as phoneNum, cardNo as idNum, updateTime, createTime from student;";
//
//        String url2 = "jdbc:mysql://localhost:3306/demo00?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
//        String username2 = "root";
//        String password2 = "w19890528";
//        String sql2 = "select id as baiYunId, phone as phoneNum, card as idNum, updateTime, createTime from user;";

    private String url1 = "jdbc:mysql://rm-bp1io2x37e97r2mzuo.mysql.rds.aliyuncs.com:3306/recommentation_dev" +
            "?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
    private String username1 = "adatafun";
    private String password1 = "adatafun@dp2017";
    private String sql1 = "select id as longTengId, phone_str as phoneNum, alipay_user_id as alipayId, email_str as email," +
            " lastupdatetime as updateTime, cdate as createTime from tb_user where cdate >= ? and cdate < ?;";

    private String url2 = "jdbc:mysql://rm-bp1741c4qix10l3czo.mysql.rds.aliyuncs.com:3306/airport_cloud_test" +
            "?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
    private String username2 = "airport";
    private String password2 = "Wyun4test";
    private String sql2 = "select wx_client_id as baiYunId, client_phone as phoneNum, client_identity_card as idNum," +
            " client_passport_number as passportNum, update_time as updateTime, create_time as createTime " +
            "from client_info where create_time >= ? and create_time < ?;";

    private String url3 = "jdbc:mysql://39.108.204.83:3306/app_track?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
    private String username3 = "track_read2";
    private String password3 = "track_read_6688D";
    private String sql3 = "select user_id as longTengId, DATE_FORMAT(create_time,\"%Y-%m-%d\") as createTime from tbd_app_heart_beat " +
            "where user_id is not null and user_id != '' and create_time >= ? and create_time < ? group by user_id," +
            " DATE_FORMAT(create_time,\"%Y-%m-%d\");";

    public MysqlConf() {
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl1() {
        return url1;
    }

    public void setUrl1(String url1) {
        this.url1 = url1;
    }

    public String getUsername1() {
        return username1;
    }

    public void setUsername1(String username1) {
        this.username1 = username1;
    }

    public String getPassword1() {
        return password1;
    }

    public void setPassword1(String password1) {
        this.password1 = password1;
    }

    public String getSql1() {
        return sql1;
    }

    public void setSql1(String sql1) {
        this.sql1 = sql1;
    }

    public String getUrl2() {
        return url2;
    }

    public void setUrl2(String url2) {
        this.url2 = url2;
    }

    public String getUsername2() {
        return username2;
    }

    public void setUsername2(String username2) {
        this.username2 = username2;
    }

    public String getPassword2() {
        return password2;
    }

    public void setPassword2(String password2) {
        this.password2 = password2;
    }

    public String getSql2() {
        return sql2;
    }

    public void setSql2(String sql2) {
        this.sql2 = sql2;
    }

    public String getUrl3() {
        return url3;
    }

    public void setUrl3(String url3) {
        this.url3 = url3;
    }

    public String getUsername3() {
        return username3;
    }

    public void setUsername3(String username3) {
        this.username3 = username3;
    }

    public String getPassword3() {
        return password3;
    }

    public void setPassword3(String password3) {
        this.password3 = password3;
    }

    public String getSql3() {
        return sql3;
    }

    public void setSql3(String sql3) {
        this.sql3 = sql3;
    }
}
