package com.adatafun.computing.platform.util;

import com.adatafun.computing.platform.model.PlatformUser;

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
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            AesUtil aes = new AesUtil("fengshu_20170228");
            while (resultSet.next()) {
                PlatformUser value = new PlatformUser();
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = resultSetMetaData.getColumnLabel(i);
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue != null) {
                        map.put(columnName, columnValue);
                    } else if (!columnName.equals("updateTime") && !columnName.equals("createTime")) {
                        map.put(columnName, "");
                    } else {
                        map.put(columnName, "1000-01-01 00:00:00");
                    }
                }
                if (map.containsKey("phoneNum")) {
                    value.setPhoneNum(map.get("phoneNum").toString());
                } else {
                    value.setPhoneNum("");
                }
                if (map.containsKey("deviceNum")) {
                    value.setDeviceNum(map.get("deviceNum").toString());
                } else {
                    value.setDeviceNum("");
                }
                if (map.containsKey("idNum")) {
                    value.setIdNum(map.get("idNum").toString());
                } else {
                    value.setIdNum("");
                }
                if (map.containsKey("passportNum")) {
                    value.setPassportNum(map.get("passportNum").toString());
                } else {
                    value.setPassportNum("");
                }
                if (map.containsKey("longTengId")) {
                    value.setLongTengId(map.get("longTengId").toString());
                } else {
                    value.setLongTengId("");
                }
                if (map.containsKey("baiYunId")) {
                    value.setBaiYunId(map.get("baiYunId").toString());
                } else {
                    value.setBaiYunId("");
                }
                if (map.containsKey("alipayId")) {
                    value.setAlipayId(map.get("alipayId").toString());
                } else {
                    value.setAlipayId("");
                }
                if (map.containsKey("email")) {
                    value.setEmail(map.get("email").toString());
                } else {
                    value.setEmail("");
                }
                if (map.containsKey("updateTime")) {
                    Timestamp updateTime = new Timestamp(simpleDateFormat.parse(map.get("updateTime").toString()).getTime());
                    value.setUpdateTime(updateTime);
                }
                if (map.containsKey("createTime")) {
                    Timestamp createTime = new Timestamp(simpleDateFormat.parse(map.get("createTime").toString()).getTime());
                    value.setCreateTime(createTime);
                }
                if (!value.getLongTengId().equals("")) {
                    if (value.getPhoneNum().length()%16 == 0) {
                        value.setPhoneNum(aes.decrypt(value.getPhoneNum()));
                    }
                    if (value.getPassportNum().length()%16 == 0) {
                        value.setPassportNum(aes.decrypt(value.getPassportNum()));
                    }
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
