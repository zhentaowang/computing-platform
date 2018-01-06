package com.adatafun.computing.platform.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * TableComputingService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/25.
 */
public class TableComputingService {

    public static void main(String[] args) {

//		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
//		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.BIG_INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.BIG_INT_TYPE_INFO,
				BasicTypeInfo.DATE_TYPE_INFO,
				BasicTypeInfo.DATE_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
		};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("com.mysql.jdbc.Driver")
				.setDBUrl("jdbc:mysql://rm-bp150tq91vm9u7n3ho.mysql.rds.aliyuncs.com:3306/recommendation_dev?useUnicode=true&characterEncoding=utf-8")
				.setUsername("wyunadmin")
				.setPassword("TSLKtPpP2nD3EV")
				.setQuery("select * from tbd_user_flight")
				.setRowTypeInfo(rowTypeInfo)
				.finish();
		System.out.println("fdfdfd");

//		DataSet<RowTypeInfo> input = rowTypeInfo;
//
//		Table table = tEnv.fromDataSet(input);
//
//		Table filtered = table
//				.groupBy("word")
//				.select("word, frequency.sum as frequency")
//				.filter("frequency = 2");
//
//		DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);


    }
}
