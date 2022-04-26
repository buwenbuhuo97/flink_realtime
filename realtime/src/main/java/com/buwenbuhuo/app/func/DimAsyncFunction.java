package com.buwenbuhuo.app.func;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.common.GmallConfig;
import com.buwenbuhuo.util.DimUtil;
import com.buwenbuhuo.util.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Author 不温卜火
 * Create 2022-04-26 15:18
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:异步IO实现
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {

            @SneakyThrows
            @Override
            public void run() {
                // 1.查询维表数据
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input));

                // 2.将维表数据补充到JavaBean中
                join(input, dimInfo);

                // 3.将补充之后的数据输出
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 再次查询补充信息
        System.out.println("TimeOut:" + input);
    }
}
