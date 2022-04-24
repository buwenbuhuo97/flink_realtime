package com.buwenbuhuo.app.dws;

import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-24 15:04
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域品牌-品类-用户粒度退单各窗口汇总表代码实现
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2.从 Kafka dwd_trade_order_refund 主题读取退单明细数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window_app";
        DataStreamSource<String> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));




        // TODO 3.

        // TODO 4.

        // TODO 5.

        // TODO 6.

        // TODO 7.

        // TODO 8.启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");

    }
}
