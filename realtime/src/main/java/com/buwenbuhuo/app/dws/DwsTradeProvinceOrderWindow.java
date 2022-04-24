package com.buwenbuhuo.app.dws;

import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.ZoneId;

/**
 * Author 不温卜火
 * Create 2022-04-24 14:49
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域省份粒度下单各窗口汇总表代码实现
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.从 Kafka dwd_trade_order_detail 主题读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window_app";
        DataStreamSource<String> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));



        // TODO 3.读取购物车表数据

        // TODO 4.

        // TODO 5.

        // TODO 6.

        // TODO 7.

        // TODO 12.启动任务
        env.execute("DwsTradeProvinceOrderWindow");

    }
}
