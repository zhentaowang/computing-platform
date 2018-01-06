/**
 * TbdUserFlight.java
 * Copyright(C) 2016 杭州量子金融信息服务有限公司
 * https://www.zhiweicloud.com
 * 2017-10-18 11:58:27 Created By wzt
*/
package com.adatafun.computing.platform.indexMap;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.util.Date;

/**
 * TbdUserFlight.java
 * Copyright(C) 2016 杭州量子金融信息服务有限公司
 * https://www.zhiweicloud.com
 * 2017-10-18 11:58:27 Created By wzt
*/
@ApiModel(value="TbdUserFlight",description="tbd_user_flight")
public class TbdUserFlight {
    @ApiModelProperty(value="",name="id", required=true)
    @NotEmpty
    @Id
    @GeneratedValue(generator = "JDBC")
    private String id;

    @ApiModelProperty(value="",name="userId")
    private Long userId;

    @ApiModelProperty(value="",name="flightInfoId")
    private String flightInfoId;

    @ApiModelProperty(value="",name="status")
    private Long status;

    @ApiModelProperty(value="",name="createTime")
    private Date createTime;

    @ApiModelProperty(value="",name="updateTime")
    private Date updateTime;

    @ApiModelProperty(value="",name="identity")
    private String identity;

    /**
     * 
     * @return id 
     */
    public String getId() {
        return id;
    }

    /**
     * 
     * @param id 
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * 
     * @return user_id 
     */
    public Long getUserId() {
        return userId;
    }

    /**
     * 
     * @param userId 
     */
    public void setUserId(Long userId) {
        this.userId = userId;
    }

    /**
     * 
     * @return flight_info_id 
     */
    public String getFlightInfoId() {
        return flightInfoId;
    }

    /**
     * 
     * @param flightInfoId 
     */
    public void setFlightInfoId(String flightInfoId) {
        this.flightInfoId = flightInfoId;
    }

    /**
     * 
     * @return status 
     */
    public Long getStatus() {
        return status;
    }

    /**
     * 
     * @param status 
     */
    public void setStatus(Long status) {
        this.status = status;
    }

    /**
     * 
     * @return create_time 
     */
    public Date getCreateTime() {
        return createTime;
    }

    /**
     * 
     * @param createTime 
     */
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    /**
     * 
     * @return update_time 
     */
    public Date getUpdateTime() {
        return updateTime;
    }

    /**
     * 
     * @param updateTime 
     */
    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * 
     * @return identity 
     */
    public String getIdentity() {
        return identity;
    }

    /**
     * 
     * @param identity 
     */
    public void setIdentity(String identity) {
        this.identity = identity;
    }
}