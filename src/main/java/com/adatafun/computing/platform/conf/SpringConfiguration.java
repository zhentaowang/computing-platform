/*
 * Copyright (c) 2017. Hangzhou FenShu Tech Co., Ltd. All rights reserved.
 * Created by wangzhentao@iairportcloud.com on 2016/09/02
 */

package com.adatafun.computing.platform.conf;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * SpringConfiguration.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 05/09/2017.
 */
@Configuration
@PropertySource({"classpath:application.properties"})
@ComponentScan("com.adatafun.computing.platform")
public class SpringConfiguration {
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}

