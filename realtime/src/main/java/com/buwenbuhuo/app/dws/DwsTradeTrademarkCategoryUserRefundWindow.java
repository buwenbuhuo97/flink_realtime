package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.app.func.DimAsyncFunction;
import com.buwenbuhuo.bean.TradeTrademarkCategoryUserRefundBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import com.buwenbuhuo.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.concurrent.TimeUnit;

/**
 * Author 不温卜火
 * Create 2022-04-24 15:04
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域品牌-品类-用户粒度退单各窗口汇总表代码实现
 * 去重set自动去重 ，在TradeProvinceOrderWindow实体类 补充订单 ID 集合，用于统计下单次数
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2.从 Kafka dwd_trade_order_refund 主题读取退单明细数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window_app";
        DataStreamSource<String> userRefundlDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3.过滤数据并转换为JSON对象
        SingleOutputStreamOperator<String> filteredDS = userRefundlDS.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String provinceId = jsonObj.getString("province_id");
                            if (provinceId != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);


        // TODO 4.按照唯一键分组,主键为"id"
        KeyedStream<JSONObject, String> keyedByStream = mappedStream.keyBy(json -> json.getString("id"));

        // TODO 5. 去重
        SingleOutputStreamOperator<JSONObject> processedDS = keyedByStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            // 定义状态
            private ValueState<JSONObject> lastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastValueState = getRuntimeContext().getState(
                        new ValueStateDescriptor<JSONObject>("last-value-state", JSONObject.class)
                );
            }


            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastValue = lastValueState.value();
                if (lastValue == null) {
                    ctx.timerService().registerProcessingTimeTimer(5000L);
                    // 更新数据
                    lastValueState.update(jsonObj);
                } else {
                    String lastRowOpTs = lastValue.getString("row_op_ts");
                    String rowOpTs = jsonObj.getString("row_op_ts");
                    if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                        lastValueState.update(jsonObj);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastValue = this.lastValueState.value();
                if (lastValue != null) {
                    out.collect(lastValue);
                }
                lastValueState.clear();
            }
        });

        // TODO 6.转换数据结构
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> JaveBeanDS = processedDS.map(json -> {
            String orderId = json.getString("order_id");
            String userId = json.getString("user_id");
            String skuId = json.getString("sku_id");
            Long ts = json.getLong("ts");

            TradeTrademarkCategoryUserRefundBean trademarkCategoryUserRefundBean = TradeTrademarkCategoryUserRefundBean.builder()
                    .refundCount(1L)
                    .userId(userId)
                    .skuId(skuId)
                    .ts(ts)
                    .build();

            return trademarkCategoryUserRefundBean;
        });

        // 测试打印
        JaveBeanDS.print("JaveBeanDS>>>>>>");


        // TODO 7.关联维表信息
        // 7.1 关联SKU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                JaveBeanDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 测试打印
        withSkuInfoDS.print("withSkuInfoDS>>>>>>");

        // 7.2 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withTrademarkDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 测试打印
        withTrademarkDS.print("withTrademarkDS>>>>>>");

        // 7.3 关联Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory3DS = AsyncDataStream.unorderedWait(
                withTrademarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 测试打印
        withCategory3DS.print("withCategory3DS>>>>>>");


        // 7.4 关联Category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 测试打印
        withCategory2DS.print("withCategory2DS>>>>>>");


        // 7.5 关联Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 测试打印
        withCategory1DS.print("withCategory1DS>>>>>>");


        // TODO 8.设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withWaterMarkDS = withCategory1DS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <TradeTrademarkCategoryUserRefundBean>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                return element.getTs() * 1000;
            }
        }));


        // TODO 9.分组、开窗、聚合
        // 9.1 分组
        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> keyedAggDS = withWaterMarkDS.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                return value.getTrademarkId() +
                        value.getTrademarkName() +
                        value.getTrademarkId() +
                        value.getCategory1Name() +
                        value.getCategory2Id() +
                        value.getCategory2Name() +
                        value.getCategory3Id() +
                        value.getCategory3Name() +
                        value.getUserId();
            }
        });

        // 9.2 开窗
        WindowedStream<TradeTrademarkCategoryUserRefundBean, String, TimeWindow> windowDS = keyedAggDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 9.3 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultDS = windowDS.reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());
                return value1;
            }
        }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {

                // 获取数据
                TradeTrademarkCategoryUserRefundBean userRefundBean = input.iterator().next();

                // 补充信息
                userRefundBean.setTs(System.currentTimeMillis());
                userRefundBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                userRefundBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                // 输出数据
                out.collect(userRefundBean);
            }
        });

        // TODO 10.将数据写入到ClickHouse
        // TODO 7.输出打印
        resultDS.print(">>>>>>");

        // TODO 8.将数据写出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        // TODO 11.启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");

    }
}
