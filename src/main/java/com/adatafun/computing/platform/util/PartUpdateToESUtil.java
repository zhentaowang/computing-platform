package com.adatafun.computing.platform.util;

import com.google.gson.Gson;
import io.searchbox.action.BulkableAction;
import io.searchbox.action.SingleResultAbstractDocumentTargetedAction;
import io.searchbox.core.DocumentResult;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * PartUpdateToESUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/18.
 */
public class PartUpdateToESUtil extends SingleResultAbstractDocumentTargetedAction implements BulkableAction<DocumentResult> {

    private PartUpdateToESUtil(Builder builder) {
        super(builder);

        this.payload = builder.payload;
        setURI(buildURI());
    }

    @Override
    public String getBulkMethodName() {
        return "update";
    }

    @Override
    protected String buildURI() {
        return super.buildURI() + "/_update";
    }

    @Override
    public String getData(Gson gson) {
        String str = super.getData(gson);
        if(getId()!=null){
            str = "{\"doc\":"+str+"}";
        }
        return str;
    }

    @Override
    public String getRestMethodName() {
        return "POST";
    }

    @Override
    public String getPathToResult() {
        return "ok";
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }

        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .isEquals();
    }

    public static class Builder extends SingleResultAbstractDocumentTargetedAction.Builder<PartUpdateToESUtil, Builder> {
        private final Object payload;

        Builder(Object payload) {
            this.payload = payload;
        }

        public PartUpdateToESUtil build() {
            return new PartUpdateToESUtil(this);
        }
    }

}
