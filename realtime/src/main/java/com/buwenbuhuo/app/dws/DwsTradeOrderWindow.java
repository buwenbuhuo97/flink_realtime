package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.app.func.OrderDetailFilterFunction;
import com.buwenbuhuo.bean.TradeOrderBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * Author 不温卜火
 * Create 2022-04-24 0:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域下单各窗口汇总表代码实现
 */
public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.从 Kafka dwd_trade_order_detail 读取订单明细数据
        String groupId = "dws_trade_order_window_app";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDs = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);

        // TODO 3.提取时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = orderDetailJsonObjDs.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        String createTime = element.getString("order_create_time");
                        return DateFormatUtil.toTs(createTime, true);
                    }
                }));

        // TODO 4.按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidStream = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));

        // TODO 5.提取下单独立用户并转换为JavaBean对象
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDS = keyedByUidStream.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {

            private ValueState<String> lastOrderDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                // 取出状态时间
                String lastOrder = lastOrderDt.value();

                // 获取当前数据下单日期
                String curDt = value.getString("order_create_time").split("")[0];
                // 测试打印
                // System.out.println(lastOrder + "---------" + curDt);

                // 定义独立下单数以及新增下单数
                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;

                // 定义独立下单数及新增下单数
                if (lastOrder == null) {
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;

                    lastOrderDt.update(curDt);
                } else if (!lastOrder.equals(curDt)) {
                    orderUniqueUserCount = 1L;

                    lastOrderDt.update(curDt);
                }

                // 输出数据
                Double activityReduceAmount = value.getDouble("activity_reduce_amount");
                if (activityReduceAmount == null) {
                    activityReduceAmount = 0.0D;
                }

                Double couponReduceAmount = value.getDouble("coupon_reduce_amount");
                if (couponReduceAmount == null) {
                    couponReduceAmount = 0.0D;
                }

                out.collect(new TradeOrderBean("", "",
                        orderUniqueUserCount,
                        orderNewUserCount,
                        activityReduceAmount,
                        couponReduceAmount,
                        value.getDouble("original_total_amount"),
                        0L
                ));
            }
        });

        // TODO 6.开窗、聚合
        AllWindowedStream<TradeOrderBean, TimeWindow> windowedStream = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<TradeOrderBean> resultDS = windowedStream.reduce(new ReduceFunction<TradeOrderBean>() {
            @Override
            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value1.getOrderUniqueUserCount());
                value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());
                return value1;
            }
        }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                // 取出数据
                TradeOrderBean orderBean = values.iterator().next();

                // 补充时间
                orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                orderBean.setTs(System.currentTimeMillis());

                // 输出数据
                out.collect(orderBean);
            }
        });

        // TODO 7.打印数据
        resultDS.print(">>>>>>");

        // TODO 8.将数据输出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));

        // TODO 9.启动任务
        env.execute("DwsTradeOrderWindow");
    }
}
