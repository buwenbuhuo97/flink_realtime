package com.buwenbuhuo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.common.GmallConfig;
import redis.clients.jedis.Jedis;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * Author 不温卜火
 * Create 2022-04-26 10:20
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 维度查询工具类，JDBC工具类再封装，仅本项目当中使用
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        // 1. 获取Redis连接
        Jedis jedis = JedisUtil.getJedis();

        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoStr = jedis.get(redisKey);
        if (dimInfoStr != null) {
            // 重置数据的过期日期
            jedis.expire(redisKey, 24 * 60 * 60);
            // 归还连接
            jedis.close();
            // 返回结果
            return JSON.parseObject(dimInfoStr);
        }

        // 2.拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id ='" + id + "'";
        System.out.println("查询SQL为：" + querySql);

        // 3.查询Phoenix
        List<JSONObject> list = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfo = list.get(0);

        // 4.将数据写入Redis
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        // 5.返回结果数据
        return list.get(0);
    }

    // 删除维表数据
    public static void delDimInfo(String tableName, String id) {
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }


    // 测试类
    public static void main(String[] args) throws Exception {
        // 创建连接
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();

        // 插入表名 & id
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_TRADEMARK", "25");
        long end = System.currentTimeMillis();

        // 再次插入表名 & id
        JSONObject dimInfo1 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "25");
        long end1 = System.currentTimeMillis();

        // 第三次插入表名 & id
        JSONObject dimInfo2 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "25");
        long end2 = System.currentTimeMillis();

        // 测试
        System.out.println("初始查询时间  ：" + (end - start));
        System.out.println("再次查询时间  ：" + (end1 - end));
        System.out.println("第三次查询时间：" + (end2 - end1));

        // 输出打印，三次查询时间不同
        System.out.println(dimInfo);
        System.out.println(dimInfo1);
        System.out.println(dimInfo2);

        // 关闭连接
        connection.close();
    }
}
