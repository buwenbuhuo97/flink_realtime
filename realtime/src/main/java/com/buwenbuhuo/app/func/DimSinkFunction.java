package com.buwenbuhuo.app.func;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * Author 不温卜火
 * Create 2022-04-13 10:06
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 自定义DimSinkFunction类
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    /**
     * 主流Value:
     * {
     * "sinkTable":"dim_xxx",
     * "database":"gmall","table":"base_trademark","type":"update",
     * "data":{"id":100924,"tm_name":"buwenbuhuo","old":{}}
     * }
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            // 拼接SQL upsert into db.tn(id,tm_name) values('100924','buwenbuhuo')
            String upsertSql = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("data"));
            System.out.println(upsertSql);

            // 预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 执行写入操作
            preparedStatement.execute();
            // connection.setAutoCommit(true);
            connection.commit();
        } catch (SQLException e) {
            System.out.println("插入数据失败！");
        } finally {
            // 释放资源
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * @param sinkTable tn
     * @param data      {"id":100924,"tm_name":"buwenbuhuo"}
     * @return upsert into db.tn(id,tm_name) values('100924','buwenbuhuo')
     */
    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        // Scala : list.mkString(",")  ["1","2","3"] => "1,2,3"
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns,",") +  ") values('" +
                StringUtils.join(values,"','") + "')";
    }
}

