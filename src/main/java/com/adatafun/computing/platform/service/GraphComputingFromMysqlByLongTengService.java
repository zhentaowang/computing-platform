package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.conf.MysqlConf;
import com.adatafun.computing.platform.io.DataSetInputFromMysqlParam;
import com.adatafun.computing.platform.io.DataSetOutputToElasticSearchByDay;
import com.adatafun.computing.platform.model.PlatformUser;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * GraphComputingFromMysqlByLongTengService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/18.
 */
public class GraphComputingFromMysqlByLongTengService {

    public static void main(String[] args) throws Exception {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//HH:24小时制，hh:12小时制

        String minDate_long = "2012-10-26 00:00:00";
        Calendar calendar_long = new GregorianCalendar();
        calendar_long.setTime(simpleDateFormat.parse(minDate_long));
        String startTime_long;
        String stopTime_long = simpleDateFormat.format(calendar_long.getTime());

        MysqlConf mysqlConf = new MysqlConf();

        for (int m = 0; m < 16; m++) {

            startTime_long = stopTime_long;
            calendar_long.add(Calendar.MONTH, 4);//把日期往后增加一个月.正数往后推,负数往前移动
            stopTime_long = simpleDateFormat.format(calendar_long.getTime());

            String sql = "select id as longTengId, phone_str as phoneNum, alipay_user_id as alipayId, email_str as email," +
                    " lastupdatetime as updateTime, cdate as createTime from tb_user where cdate >= ? and cdate < ?;";
            DataSetInputFromMysqlParam dataSetInput1 = new DataSetInputFromMysqlParam(mysqlConf.getDriver(), mysqlConf.getUrl1(),
                    mysqlConf.getUsername1(), mysqlConf.getPassword1(), sql, startTime_long, stopTime_long);
            List<PlatformUser> studentList = dataSetInput1.readFromMysql();
            if (studentList.size() == 0) {
                continue;
            }
            System.out.println("lt connect successful");

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.fromCollection(studentList).output(new DataSetOutputToElasticSearchByDay("dmp-user", "dmp-user"));
            env.execute();
//            Thread.sleep(1000);

        }
        Thread.sleep(100000);
    }

}
