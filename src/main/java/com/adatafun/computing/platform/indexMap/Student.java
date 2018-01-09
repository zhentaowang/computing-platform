package com.adatafun.computing.platform.indexMap;

import java.sql.Timestamp;

/**
 * Student.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/26.
 */
public class Student {

    private Integer studentId;
    private String studentName;
    private String address;
    private String sex;
    private String mobileNo;
    private String cardNo;
    private Timestamp update;
    private Timestamp create;

    public Student() {
    }

    public Student(Integer studentId, String studentName, String address, String sex, String mobileNo, String cardNo) {
        this.studentId = studentId;
        this.studentName = studentName;
        this.address = address;
        this.sex = sex;
        this.mobileNo = mobileNo;
        this.cardNo = cardNo;
    }

    public Student(Integer studentId, String studentName, String address, String sex, String mobileNo, String cardNo, Timestamp update, Timestamp create) {
        this.studentId = studentId;
        this.studentName = studentName;
        this.address = address;
        this.sex = sex;
        this.mobileNo = mobileNo;
        this.cardNo = cardNo;
        this.update = update;
        this.create = create;
    }

    public Integer getStudentId() {
        return studentId;
    }

    public void setStudentId(Integer studentId) {
        this.studentId = studentId;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getMobileNo() {
        return mobileNo;
    }

    public void setMobileNo(String mobileNo) {
        this.mobileNo = mobileNo;
    }

    public String getCardNo() {
        return cardNo;
    }

    public void setCardNo(String cardNo) {
        this.cardNo = cardNo;
    }

    public Timestamp getUpdate() {
        return update;
    }

    public void setUpdate(Timestamp update) {
        this.update = update;
    }

    public Timestamp getCreate() {
        return create;
    }

    public void setCreate(Timestamp create) {
        this.create = create;
    }
}
