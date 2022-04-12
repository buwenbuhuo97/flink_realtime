package com.buwenbuhuo.common;

/**
 * Author 不温卜火
 * Create 2022-04-12 21:48
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:配置常量类
 */
public class GmallConfig {
    /**
     * HBASE_SCHEMA     : Phoenix库名
     * PHOENIX_DRIVER   : Phoenix驱动
     * PHOENIX_SERVER   : Phoenix连接参数
     */
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181";
}
