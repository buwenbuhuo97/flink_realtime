package com.buwenbuhuo.util;


import com.buwenbuhuo.bean.TransientSink;
import com.buwenbuhuo.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * Author 不温卜火
 * Create 2022-04-22 10:44
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:ClickHouse 工具类
 */
public class MyClickHouseUtil {
    /**
     *
     * @param sql
     * @param <T> 这个规定就表示T是一个具体类型
     * @return
     */
    public static <T> SinkFunction<T> getClickHouseSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 使用反射的方式提取字段
                        Class<?> clz = t.getClass();
                        Field[] fields = clz.getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }
                            field.setAccessible(true);
                            try {
                                // 获取数据并给占位符赋值
                                Object value = field.get(t);
                                preparedStatement.setObject(i + 1 - offset, value);
                            } catch (IllegalAccessException e) {
                                System.out.println("ClickHouse 数据插入 SQL 占位符传参异常 ~");
                                e.printStackTrace();
                            }
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000L)
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
