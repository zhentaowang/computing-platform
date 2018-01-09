package com.adatafun.computing.platform.service;

import com.adatafun.computing.platform.indexMap.PlatformUserDemo;
import com.adatafun.computing.platform.indexMap.Student;
import com.adatafun.computing.platform.indexMap.User;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * BitchTableFromMysqlService.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2017/12/25.
 */
public class BitchTableFromMysqlService {

    public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Student> input1 = env.fromElements(
				new Student(1, "小张", "留下里", "男", "18858272535", ""),
				new Student(114, "小花", "仓前", "女", "", "455451445"));
		DataSet<User> input2 = env.fromElements(
				new User(1, "小张", "18858272535", "46935"),
				new User(3, "小花", "15900000000", "455451445"));
		Table table1 = tableEnv.fromDataSet(input1, "studentId, mobileNo, cardNo");
		Table result1 = table1.as("a, b, c");
		Table table2 = tableEnv.fromDataSet(input2, "id, phone, card");
		Table result2 = table2.as("d, e, f");
		Table table = result1.join(result2).where("b = e").select("e as phoneNum, f as idNum, " +
				"a as longTengId, d as baiYunId");
		DataSet<PlatformUserDemo> result = tableEnv.toDataSet(table, PlatformUserDemo.class);
		result.print();
    }
}
