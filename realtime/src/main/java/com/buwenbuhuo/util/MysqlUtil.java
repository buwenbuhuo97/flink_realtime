package com.buwenbuhuo.util;

/**
 * Author 不温卜火
 * Create 2022-04-14 20:44
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 此工具类，用于维度退化
 */
public class MysqlUtil {

    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://hadoop01:3306/gmall', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '100', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = '123456', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }
}
