package com.buwenbuhuo.app.dwd.db;

import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * Author 不温卜火
 * Create 2022-04-14 20:46
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域加购事务事实表实现类
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设定Table中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GTM+8"));

        // TODO 2.从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db\n" +
                "(\n" +
                "    database string ,\n" +
                "    table    string ,\n" +
                "    type     string ,\n" +
                "    data     map<string,string> ,\n" +
                "    old      map<string,string> ,\n" +
                "    ts       string ,\n" +
                "    proc_time as proactive\n" +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db","dwd_trade_cart_add"));


        // TODO 3.读取购物车表数据
        tableEnv.sqlQuery("");

        // TODO 4.

        // TODO 5.

        // TODO 6.

        // TODO 7.

        // TODO 8.


    }
}
