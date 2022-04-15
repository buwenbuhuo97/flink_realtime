package com.buwenbuhuo.util;

/**
 * Author 不温卜火
 * Create 2022-04-14 20:44
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 此工具类，用于维度退化
 */
public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {

        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {

        String ddl = "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://hadoop01:3306/gmall',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = 'root',\n" +
                "'password' = '123456',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
        return ddl;
    }
}
