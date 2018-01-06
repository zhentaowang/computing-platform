package com.adatafun.computing.platform.indexMap;

/**
 * XXX.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/27.
 */
public class User {

    private String name;
    private String phone;
    private String card;

    public User() {
    }

    public User(String name, String phone, String card) {
        this.name = name;
        this.phone = phone;
        this.card = card;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}
