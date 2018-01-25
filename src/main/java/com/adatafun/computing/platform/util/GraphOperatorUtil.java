package com.adatafun.computing.platform.util;

import com.adatafun.computing.platform.model.PlatformUser;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * GraphOperatorUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/17.
 */
public class GraphOperatorUtil {

    public PlatformUser vertexMerge(PlatformUser u1, PlatformUser u2) {
        Boolean isMatched = (!u1.getPhoneNum().equals("") && u1.getPhoneNum().equals(u2.getPhoneNum()))
                || (!u1.getDeviceNum().equals("") && u1.getDeviceNum().equals(u2.getDeviceNum()))
                || (!u1.getIdNum().equals("") && u1.getIdNum().equals(u2.getIdNum()))
                || (!u1.getPassportNum().equals("") && u1.getPassportNum().equals(u2.getPassportNum()))
                || (!u1.getEmail().equals("") && u1.getEmail().equals(u2.getEmail()));
        if (isMatched) {
            if (u1.getPhoneNum().equals("") && !u2.getPhoneNum().equals("")) {
                u1.setPhoneNum(u2.getPhoneNum());
            }
            if (u1.getDeviceNum().equals("") && !u2.getDeviceNum().equals("")) {
                u1.setDeviceNum(u2.getDeviceNum());
            }
            if (u1.getIdNum().equals("") && !u2.getIdNum().equals("")) {
                u1.setIdNum(u2.getIdNum());
            }
            if (u1.getPassportNum().equals("") && !u2.getPassportNum().equals("")) {
                u1.setPassportNum(u2.getPassportNum());
            }
            if (u1.getEmail().equals("") && !u2.getEmail().equals("")) {
                u1.setEmail(u2.getEmail());
            }
            u1.setBaiYunId(u2.getBaiYunId());
        }
        u1.setCreateTime(new Timestamp(new Date().getTime()));
        u1.setUpdateTime(new Timestamp(new Date().getTime()));
        u1.setId(u1.getLongTengId() + '*' + u1.getBaiYunId());
        return u1;
    }

    public List<PlatformUser> vertexRemove(List<PlatformUser> userList_by, List<PlatformUser> platformUserList) {
        for (PlatformUser user : userList_by) {
            int flag = 0;
            for (PlatformUser platformUser : platformUserList) {
                if (user.getBaiYunId().equals(platformUser.getBaiYunId())) {
                    flag = 1;
                    break;
                }
            }
            if (flag == 0) {
                user.setCreateTime(new Timestamp(new Date().getTime()));
                user.setUpdateTime(new Timestamp(new Date().getTime()));
                user.setId('*' + user.getBaiYunId());
                platformUserList.add(user);
            }
        }
        return platformUserList;
    }

}
