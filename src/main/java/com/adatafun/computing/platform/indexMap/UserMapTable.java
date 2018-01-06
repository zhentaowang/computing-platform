package com.adatafun.computing.platform.indexMap;

/**
 * UserMapTable.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/5.
 */
public class UserMapTable<T> {

    private T value;

    public UserMapTable(T value) {
        this.value=value;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

}
