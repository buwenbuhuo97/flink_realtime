package com.buwenbuhuo.app.dwd.db;

import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-19 21:38
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域取消订单事务事实表代码实现
 */
public class DwdTradeCancelDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.使用DDL的方式读取 Kafka dwd_trade_order_detail 主题的数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail_table( " +
                "    `order_detail_id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `order_create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_original_amount` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `pt` TIMESTAMP_LTZ(3), " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `activity_create_time` string , " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `coupon_create_time` string , " +
                "    `dic_name` string " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_order_cancel_app"));

        // TODO 3.过滤出取消订单数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    * " +
                "from dwd_trade_order_detail_table " +
                "where `type`='update' " +
                "and `old`['order_status'] is not null " +
                "and order_status='1003'");
        tableEnv.createTemporaryView("filter_table", filterTable);

        // TODO 4.创建Kafka下单数据表
        tableEnv.executeSql("" +
                "create table dwd_trade_cancel_detail_table ( " +
                "    `order_detail_id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `order_create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_original_amount` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `pt` TIMESTAMP_LTZ(3), " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `activity_create_time` string , " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `coupon_create_time` string , " +
                "    `dic_name` string " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_cancel_detail", ""));


        // TODO 5.将数据写出到Kafka
        tableEnv.executeSql("insert into dwd_trade_cancel_detail_table select * from filter_table")
                .print();

        //TODO 12.启动任务
        env.execute("DwdTradeCancelDetail");
    }
}
