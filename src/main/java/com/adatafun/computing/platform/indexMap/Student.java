package com.adatafun.computing.platform.indexMap;

/**
 * XXX.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/26.
 */
public class Student {

    private String name;
    private String address;
    private String sex;

    public Student() {
    }

    public Student(String name, String address, String sex) {
        this.name = name;
        this.address = address;
        this.sex = sex;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}
