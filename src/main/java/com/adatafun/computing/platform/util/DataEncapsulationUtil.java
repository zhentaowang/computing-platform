package com.adatafun.computing.platform.util;

import com.adatafun.computing.platform.model.PlatformUser;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

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
            Calendar calendar = new GregorianCalendar();
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
                    if (!value.getLongTengId().equals("") && !map.get("updateTime").equals("1000-01-01 00:00:00")) {
                        Date date = DateUtils.parseDate(map.get("updateTime").toString(), new String[]{"yyyy-MM-dd"});
                        String tempTime = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
                        map.put("updateTime", tempTime);
                    }
                    calendar.setTime(simpleDateFormat.parse(map.get("updateTime").toString()));
                    calendar.add(Calendar.HOUR_OF_DAY, -8);//把日期往前减八个小时,矫正es时间差
                    Timestamp updateTime = new Timestamp(calendar.getTime().getTime());
                    value.setUpdateTime(updateTime);
                }
                if (map.containsKey("createTime")) {
                    calendar.setTime(simpleDateFormat.parse(map.get("createTime").toString()));
                    calendar.add(Calendar.HOUR_OF_DAY, -8);//把日期往前减八个小时,矫正es时间差
                    Timestamp createTime = new Timestamp(calendar.getTime().getTime());
                    value.setCreateTime(createTime);
                }
                if (value.getLongTengId().equals("")) {
                    if (value.getPhoneNum().length() == 24) {
                        value.setPhoneNum(aes.decrypt(value.getPhoneNum()));
                    }
                    if (value.getPassportNum().length() == 24) {
                        value.setPassportNum(aes.decrypt(value.getPassportNum()));
                    }
                    if (value.getIdNum().length() == 44) {
                        value.setIdNum(aes.decrypt(value.getIdNum()));
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

    public List<Map> dataEncapsulationByPartUpdate(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Map> userList = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = resultSetMetaData.getColumnLabel(i);
                    Object columnValue = resultSet.getObject(i);
                    map.put(columnName, columnValue);
                }
                userList.add(map);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple2<Long, String>> dataEncapsulationTuple2ByPartUpdate(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple2<Long, String>> userList = new ArrayList<>();
            while (resultSet.next()) {
                Tuple2<Long, String> tuple2 = new Tuple2<>();
                if (columnCount == 2){
                    Object v1 = resultSet.getObject(1);
                    Object v2 = resultSet.getObject(2);
                    if (v1 != null && !v1.equals("") && v2 != null && !v2.equals("")) {
                        Long userId = Long.valueOf(v1.toString());
                        if (userId != null) {
                            tuple2.setField(userId, 0);
                            tuple2.setField(v2.toString(), 1);
                            userList.add(tuple2);
                        }

                    }
                }
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple2<String, String>> dataEncapsulationTuple2ByAES(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple2<String, String>> userList = new ArrayList<>();
            AesUtil aes = new AesUtil("fengshu_20170228");
            while (resultSet.next()) {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue != null) {
                        if (i == 2) {
                            tuple2.setField(aes.decrypt(columnValue.toString()), i-1);
                        } else {
                            tuple2.setField(columnValue.toString(), i-1);
                        }
                    } else {
                        tuple2.setField("未知", i-1);
                    }
                }
                userList.add(tuple2);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple2<String, String>> dataEncapsulationTuple2ByPartner(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple2<String, String>> userList = new ArrayList<>();
            while (resultSet.next()) {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue != null) {
                        tuple2.setField(columnValue.toString(), i-1);
                    } else {
                        tuple2.setField("未知", i-1);
                    }
                }
                userList.add(tuple2);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple2<String, String>> dataEncapsulationTuple2ByString(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple2<String, String>> userList = new ArrayList<>();
            while (resultSet.next()) {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                tuple2.setField(resultSet.getObject(1), 0);
                String flightId = "";
                for (int i = 2; i <= columnCount; i++) {
                    Object columnValue = resultSet.getObject(i);
                    flightId += columnValue;
                }
                tuple2.setField(flightId, 1);
                userList.add(tuple2);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple6<String, String, String, String, String, String>> dataEncapsulationTuple6ByString(ResultSet resultSet)
            throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple6<String, String, String, String, String, String>> userList = new ArrayList<>();
            while (resultSet.next()) {
                Tuple6<String, String, String, String, String, String> tuple6 = new Tuple6<>();
                for (int i = 1; i <= 5; i++) {
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue != null) {
                        tuple6.setField(resultSet.getObject(i).toString(), i-1);
                    } else {
                        tuple6.setField("未知", i-1);
                    }
                }
                String flightId = "";
                for (int i = 6; i <= columnCount; i++) {
                    Object columnValue = resultSet.getObject(i);
                    flightId += columnValue;
                }
                tuple6.setField(flightId, 5);
                userList.add(tuple6);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple3<String, String, String>> dataEncapsulationTuple3ByPartUpdate(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple3<String, String, String>> userList = new ArrayList<>();
            while (resultSet.next()) {
                Tuple3<String, String, String> tuple3 = new Tuple3<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue != null) {
                        tuple3.setField(columnValue.toString(), i-1);
                    } else {
                        tuple3.setField("---", i-1);
                    }
                }
                userList.add(tuple3);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple4<String, String, String, String>> dataEncapsulationTuple4ByPartUpdate(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple4<String, String, String, String>> userList = new ArrayList<>();
            while (resultSet.next()) {
                Tuple4<String, String, String, String> tuple4 = new Tuple4<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue == null) {
                        tuple4.setField("未知", i-1);
                    } else {
                        tuple4.setField(columnValue.toString(), i-1);
                    }
                }
                userList.add(tuple4);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Tuple4<String, String, String, String>> dataEncapsulationTuple4ByPullInsert(ResultSet resultSet) throws Exception {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Tuple4<String, String, String, String>> userList = new ArrayList<>();
            AesUtil aes = new AesUtil("fengshu_20170228");
            while (resultSet.next()) {
                Tuple4<String, String, String, String> tuple4 = new Tuple4<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue == null) {
                        tuple4.setField("未知", i-1);
                    } else {
                        if (columnName.equals("client_phone") || columnName.equals("client_passport_number")
                                || columnName.equals("client_identity_card")) {
                            tuple4.setField(aes.decrypt(columnValue.toString()), i-1);
                        } else {
                            tuple4.setField(columnValue.toString(), i-1);
                        }

                    }
                }
                userList.add(tuple4);
            }
            return userList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Collector<Tuple2<String, String>> doReduceGroup(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out) {
        Tuple2<String, String> result = new Tuple2<>();
        String slots = "";
        for (Tuple2<String, String> tuple2 : values) {
            result.setField(tuple2.f0, 0);
            slots = slots + tuple2.f1 + ",";
        }
        result.setField(slots.substring(0, slots.length() - 1), 1);
        out.collect(result);
        return out;
    }

}
