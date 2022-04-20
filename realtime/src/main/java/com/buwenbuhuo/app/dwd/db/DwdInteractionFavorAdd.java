package com.buwenbuhuo.app.dwd.db;

import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-20 19:15
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:互动域收藏商品事务事实表代码实现
 */
public class DwdInteractionFavorAdd {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_favor_add_app"));

        // TODO 3. 读取收藏表数据
        Table favorInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "date_format(data['create_time'],'yyyy-MM-dd') date_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'favor_info' " +
                "and `type` = 'insert' " +
                "or (`type` = 'update' and `old`['is_cancel'] = '1' and data['is_cancel'] = '0')");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        // TODO 4. 创建 Upsert-Kafka dwd_interaction_favor_add 表
        tableEnv.executeSql("create table dwd_interaction_favor_add ( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "date_id string, " +
                "create_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_interaction_favor_add"));

        // TODO 5. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_favor_add select * from favor_info")
                .print();

        // TODO 6.启动任务
        env.execute("DwdInteractionFavorAdd");
    }
}
