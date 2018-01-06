package com.adatafun.computing.platform.indexMap;


import java.util.Date;

/**
 * PlatformUser.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/6.
 */
public class PlatformUser {

    private String phoneNum;
    private String deviceNum;
    private String idNum;
    private String passportNum;
    private String longTengId;
    private String baiYunId;
    private String alipayId;
    private String email;
    private Date updateTime;
    private Date createTime;

    public PlatformUser() {
    }

    public PlatformUser(String phoneNum, String idNum, String longTengId, String baiYunId) {
        this.phoneNum = phoneNum;
        this.idNum = idNum;
        this.longTengId = longTengId;
        this.baiYunId = baiYunId;
    }

    public PlatformUser(String phoneNum, String deviceNum, String idNum, String passportNum, String longTengId,
                        String baiYunId, String alipayId, String email) {
        this.phoneNum = phoneNum;
        this.deviceNum = deviceNum;
        this.idNum = idNum;
        this.passportNum = passportNum;
        this.longTengId = longTengId;
        this.baiYunId = baiYunId;
        this.alipayId = alipayId;
        this.email = email;
    }

    public PlatformUser(String phoneNum, String deviceNum, String idNum, String passportNum, String longTengId,
                        String baiYunId, String alipayId, String email, Date updateTime, Date createTime) {
        this.phoneNum = phoneNum;
        this.deviceNum = deviceNum;
        this.idNum = idNum;
        this.passportNum = passportNum;
        this.longTengId = longTengId;
        this.baiYunId = baiYunId;
        this.alipayId = alipayId;
        this.email = email;
        this.updateTime = updateTime;
        this.createTime = createTime;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getDeviceNum() {
        return deviceNum;
    }

    public void setDeviceNum(String deviceNum) {
        this.deviceNum = deviceNum;
    }

    public String getIdNum() {
        return idNum;
    }

    public void setIdNum(String idNum) {
        this.idNum = idNum;
    }

    public String getPassportNum() {
        return passportNum;
    }

    public void setPassportNum(String passportNum) {
        this.passportNum = passportNum;
    }

    public String getLongTengId() {
        return longTengId;
    }

    public void setLongTengId(String longTengId) {
        this.longTengId = longTengId;
    }

    public String getBaiYunId() {
        return baiYunId;
    }

    public void setBaiYunId(String baiYunId) {
        this.baiYunId = baiYunId;
    }

    public String getAlipayId() {
        return alipayId;
    }

    public void setAlipayId(String alipayId) {
        this.alipayId = alipayId;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
}
