package com.buwenbuhuo.app.dwd.db;

import com.buwenbuhuo.util.MyKafkaUtil;
import com.buwenbuhuo.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;

/**
 * Author 不温卜火
 * Create 2022-04-14 20:46
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域加购事务事实表实现类
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态存储时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // TODO 2.使用DDL方式读取Kafka topic_db 主题数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_cart_add_app"));

        // TODO 3.过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['user_id'] user_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['cart_price'] cart_price, " +
                "    if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num, " +
                "    data['sku_name'] sku_name, " +
                "    data['is_checked'] is_checked, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['is_ordered'] is_ordered, " +
                "    data['order_time'] order_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='cart_info' " +
                "and (`type`='insert'  " +
                "    or (`type`='update'  " +
                "        and `old`['sku_num'] is not null " +
                "        and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        tableEnv.createTemporaryView("cart_add", cartAddTable);


        // TODO 4.读取MySQL中的base_dic表构建维表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 5.关联两张表   维度退化
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    c.id, " +
                "    c.user_id, " +
                "    c.sku_id, " +
                "    c.cart_price, " +
                "    c.sku_num, " +
                "    c.sku_name, " +
                "    c.is_checked, " +
                "    c.create_time, " +
                "    c.operate_time, " +
                "    c.is_ordered, " +
                "    c.order_time, " +
                "    c.source_type, " +
                "    c.source_id, " +
                "    b.dic_name " +
                "from cart_add c " +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt as b " +
                "on c.source_type = b.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 6.将数据写回到Kafka DWD层
        String sinkTopic = "dwd_trade_cart_add";
        tableEnv.executeSql("" +
                "create table trade_cart_add( " +
                "    id string, " +
                "    user_id string, " +
                "    sku_id string, " +
                "    cart_price string, " +
                "    sku_num int, " +
                "    sku_name string, " +
                "    is_checked string, " +
                "    create_time string, " +
                "    operate_time string, " +
                "    is_ordered string, " +
                "    order_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    dic_name string " +
                ")" + MyKafkaUtil.getKafkaDDL(sinkTopic, ""));
        tableEnv.executeSql("insert into trade_cart_add select * from result_table")
                .print();


        // TODO 7.启动任务
        env.execute("DwdTradeCartAdd");

    }
}
