package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.app.func.DimAsyncFunction;
import com.buwenbuhuo.app.func.OrderDetailFilterFunction;
import com.buwenbuhuo.bean.TradeProvinceOrderWindow;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


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

        // TODO 2.读取DWD层order_detail数据并去重过滤
        String groupId = "dws_trade_province_order_window_app";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);


        // TODO 3.将每行数据转换为Javabean
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceOrderDS = orderDetailJsonObjDS.map(json -> new TradeProvinceOrderWindow(
                "",
                "",
                json.getString("province_id"),
                "",
                1L,
                json.getDouble("split_total_amount"),
                DateFormatUtil.toTs(json.getString("order_create_time"), true)
        ));

        // TODO 4.提取时间戳生成WaterMark
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWithWmDS = tradeProvinceOrderDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2)).
                withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindow element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        // TODO 5.分组开窗聚合
        KeyedStream<TradeProvinceOrderWindow, String> keyedStream = tradeProvinceWithWmDS.keyBy(TradeProvinceOrderWindow::getProvinceId);
        WindowedStream<TradeProvinceOrderWindow, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<TradeProvinceOrderWindow> resultDS = windowedStream.reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                return value1;
            }
        }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> input, Collector<TradeProvinceOrderWindow> out) throws Exception {
                // 获取数据
                TradeProvinceOrderWindow provinceOrderWindow = input.iterator().next();

                // 补充信息
                provinceOrderWindow.setTs(System.currentTimeMillis());
                provinceOrderWindow.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                provinceOrderWindow.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                // 输出数据
                out.collect(provinceOrderWindow);

            }
        });

        // TODO 6.关联维表获取省份名称
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWithProvinceNameDS = AsyncDataStream.unorderedWait(
                resultDS,
                new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindow input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setProvinceName(dimInfo.getString("NAME"));
                        }
                    }
                }
                , 60, TimeUnit.SECONDS);

        // TODO 7.将数据写入到ClickHouse
        tradeProvinceWithProvinceNameDS.print("tradeProvinceWithProvinceNameDS>>>>>>");
        tradeProvinceWithProvinceNameDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));


        // TODO 8.启动任务
        env.execute("DwsTradeProvinceOrderWindow");

    }
}
