package com.buwenbuhuo.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.buwenbuhuo.bean.CouponUseOrderBean;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.util.Map;
import java.util.Set;

/**
 * Author 不温卜火
 * Create 2022-04-20 19:06
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:工具域优惠券使用（下单）事务事实表代码实现
 */
public class DwdToolCouponOrder {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db` ( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`old` string, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_order_app"));

        // TODO 3. 读取优惠券领用表数据，封装为流
        Table couponUseOrder = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id, " +
                "data['using_time'] using_time, " +
                "`old`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' ");
        DataStream<CouponUseOrderBean> couponUseOrderDS = tableEnv.toAppendStream(couponUseOrder, CouponUseOrderBean.class);

        // TODO 4. 过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUseOrderBean> filteredDS = couponUseOrderDS.filter(
                couponUseOrderBean -> {
                    String old = couponUseOrderBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("using_time");
                    }
                    return false;
                }
        );
        Table resultTable = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 5. 建立 Upsert-Kafka dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_order( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "order_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_order"));

        // TODO 6. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id, " +
                "coupon_id, " +
                "user_id, " +
                "order_id, " +
                "date_id, " +
                "using_time order_time, " +
                "ts from result_table")
                .print();

        // TODO 7.启动任务
        env.execute("DwdToolCouponOrder");
    }
}
