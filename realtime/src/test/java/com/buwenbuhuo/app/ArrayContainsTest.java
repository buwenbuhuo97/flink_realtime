package com.buwenbuhuo.app;

import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-20 19:39
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
public class ArrayContainsTest {

    public static void main(String[] args) {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` string, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "test"));

        tableEnv.sqlQuery("select data,array_contains(map_keys(`old`),'aa') from topic_db")
                .execute()
                .print();

    }

}
