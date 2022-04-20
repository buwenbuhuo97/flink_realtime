package com.buwenbuhuo.app.dwd.db;

import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.ZoneId;

/**
 * Author 不温卜火
 * Create 2022-04-20 19:19
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:用户域用户注册事务事实表代码实现
 */
public class DwdUserRegister {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_detail_app"));

        // TODO 3. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select " +
                "data['id'] user_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'user_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 4. 创建 Upsert-Kafka dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`( " +
                "`user_id` string, " +
                "`date_id` string, " +
                "`create_time` string, " +
                "`ts` string, " +
                "primary key(`user_id`) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_user_register"));

        // TODO 5. 将输入写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_user_register " +
                "select  " +
                "user_id, " +
                "date_format(create_time, 'yyyy-MM-dd') date_id, " +
                "create_time, " +
                "ts " +
                "from user_info")
                .print();

        // TODO 6.启动任务
        env.execute("DwdUserRegister");
    }
}
