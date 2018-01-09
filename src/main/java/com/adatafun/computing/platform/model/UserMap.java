package com.adatafun.computing.platform.model;

import io.searchbox.annotations.JestId;

/**
 * UserMap.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/11/23.
 */
public class UserMap {

    @JestId
    private String IP;
    private String info;

    public UserMap() {
        super();
    }

    public UserMap(String IP, String info) {
        super();
        this.IP = IP;
        this.info = info;
    }

    @Override
    public String toString() {
        return "UserMap [IP=" + IP + ", info =" + info + "]";
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
}
