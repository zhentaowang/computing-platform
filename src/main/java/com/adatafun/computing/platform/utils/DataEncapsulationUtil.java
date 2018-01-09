package com.adatafun.computing.platform.utils;

import com.adatafun.computing.platform.indexMap.PlatformUser;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataEncapsulationUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/8.
 */
public class DataEncapsulationUtil {

    public List<PlatformUser> dataEncapsulation(ResultSet resultSet) throws Exception {
        try {
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
                    Timestamp updateTime = new Timestamp(simpleDateFormat.parse(map.get("updateTime").toString()).getTime());
                    value.setUpdateTime(updateTime);
                }
                if (map.containsKey("createTime")) {
                    Timestamp createTime = new Timestamp(simpleDateFormat.parse(map.get("createTime").toString()).getTime());
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

}
