package com.adatafun.computing.platform;

import com.wyun.thrift.server.server.Server;
import com.wyun.utils.SpringBeanUtil;
import org.springframework.context.support.GenericXmlApplicationContext;

/**
 * Created by wangzhentao@iairportcloud.com on 2017/09/02
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        GenericXmlApplicationContext context = new GenericXmlApplicationContext();
        context.getEnvironment().setActiveProfiles("production");
        context.setValidating(false);
        context.load( "classpath:spring.xml");
        context.refresh();
        Server server = new Server(8085);
        server.startSingleServer(SpringBeanUtil.getBean("businessService"),"businessService");
        Thread.sleep(1000000);
    }
}
