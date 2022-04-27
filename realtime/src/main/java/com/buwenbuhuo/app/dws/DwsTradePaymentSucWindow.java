package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.TradePaymentWindowBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * Author 不温卜火
 * Create 2022-04-24 0:25
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域支付各窗口汇总表代码实现
 */
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取DWD成功支付数据主题
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window_app";
        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3.过滤数据并转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = paymentStrDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 空字符串输出
                    System.out.println("value:" + value + "----");
                }
            }
        });

        // TODO 4.按照唯一键分组，支付id
        KeyedStream<JSONObject, String> keyedByDetailStream = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));


        // TODO 5.使用状态编程去重，保留第一条数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailStream.filter(new RichFilterFunction<JSONObject>() {

            // 定义状态
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value", String.class);

                // 设置描述器
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                stateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String state = valueState.value();
                if (state == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }
            }
        });

        // TODO 6.按照Uid分组
        KeyedStream<JSONObject, String> keyedByUserStream = filterDS.keyBy(json -> json.getString("user_id"));


        // TODO 7.提取当日以及总的新增支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUserStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {

            // 定义状态
            private ValueState<String> lastPaymentDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastPaymentDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-payment", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {
                // 取出状态数据
                String lastPaymentDt = lastPaymentDtState.value();

                // 取出当前数据日期
                String callbackTime = value.getString("callback_time");
                String curDt = value.getString("callback_time").split(" ")[0];

                // 定义两个数字
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;

                if (lastPaymentDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;

                    // 更新当前日期
                    lastPaymentDtState.update(curDt);
                } else if (!lastPaymentDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastPaymentDtState.update(curDt);
                }

                if (paymentSucUniqueUserCount == 1L) {
                    out.collect(new TradePaymentWindowBean(
                            "",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            DateFormatUtil.toTs(callbackTime, true)
                    ));
                }
            }
        });

        // TODO 8.提取事件事件、开窗、聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePaymentDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradePaymentWindowBean>() {
            @Override
            public long extractTimestamp(TradePaymentWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        })).windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        /**
                         * 如果使用滑动窗口，此处不能返回value1，如果使用，多个窗口数据会汇总到一起
                         */
                        return value1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                        // 取出数据
                        TradePaymentWindowBean windowBean = values.iterator().next();

                        // 补全信息
                        windowBean.setTs(System.currentTimeMillis());
                        windowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        windowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        // 输出数据
                        out.collect(windowBean);
                    }
                });

        // TODO 9.将数据写入到ClickHouse
        resultDS.print(">>>>>>");
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        // TODO 10.启动任务
        env.execute("DwsTradePaymentSucWindow");

    }
}
