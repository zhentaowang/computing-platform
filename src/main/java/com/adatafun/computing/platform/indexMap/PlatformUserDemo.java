package com.adatafun.computing.platform.indexMap;

/**
 * PlatformUserDemo.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/9.
 */
public class PlatformUserDemo {

    private String phoneNum;
    private String idNum;
    private Integer longTengId;
    private Integer baiYunId;

    public PlatformUserDemo() {
    }

    public PlatformUserDemo(String phoneNum, String idNum, Integer longTengId, Integer baiYunId) {
        this.phoneNum = phoneNum;
        this.idNum = idNum;
        this.longTengId = longTengId;
        this.baiYunId = baiYunId;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getIdNum() {
        return idNum;
    }

    public void setIdNum(String idNum) {
        this.idNum = idNum;
    }

    public Integer getLongTengId() {
        return longTengId;
    }

    public void setLongTengId(Integer longTengId) {
        this.longTengId = longTengId;
    }

    public Integer getBaiYunId() {
        return baiYunId;
    }

    public void setBaiYunId(Integer baiYunId) {
        this.baiYunId = baiYunId;
    }

    @Override
    public String toString() {
        return "PlatformUserDemo{" +
                "phoneNum='" + phoneNum + '\'' +
                ", idNum='" + idNum + '\'' +
                ", longTengId=" + longTengId +
                ", baiYunId=" + baiYunId +
                '}';
    }
}
