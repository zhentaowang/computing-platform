package com.adatafun.computing.platform.indexMap;

import java.sql.Timestamp;

/**
 * User.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/27.
 */
public class User {

    private Integer id;
    private String name;
    private String phone;
    private String card;
    private Timestamp updateTime;
    private Timestamp createTime;

    public User() {
    }

    public User(Integer id, String name, String phone, String card) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.card = card;
    }

    public User(Integer id, String name, String phone, String card, Timestamp updateTime, Timestamp createTime) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.card = card;
        this.updateTime = updateTime;
        this.createTime = createTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }
}
