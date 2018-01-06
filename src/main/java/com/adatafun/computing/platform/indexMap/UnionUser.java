package com.adatafun.computing.platform.indexMap;

/**
 * XXX.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/5.
 */
public class UnionUser {

    private String name;
    private String sex;
    private String phone;
    private String card;
    private String address;

    public UnionUser() {
    }

    public UnionUser(String name, String sex, String phone, String card, String address) {
        this.name = name;
        this.sex = sex;
        this.phone = phone;
        this.card = card;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getCard() {
        return card;
    }

    public void setCard(String card) {
        this.card = card;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
