package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.app.func.DimAsyncFunction;
import com.buwenbuhuo.app.func.OrderDetailFilterFunction;
import com.buwenbuhuo.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Author 不温卜火
 * Create 2022-04-24 0:53
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域品牌-品类-用户-SPU粒度下单各窗口汇总表代码实现
 */
public class DwsTradeTrademarkCategoryUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.获取过滤后的OrderDetail表
        String groupId = "sku_user_order_window_app";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);

        // TODO 3.转换数据为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> skuUserOrderDS = orderDetailJsonObjDS.map(json -> {

            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(json.getString("order_id"));

            return TradeTrademarkCategoryUserSpuOrderBean.builder()
                    .skuId(json.getString("sku_id"))
                    .userId(json.getString("user_id"))
                    .orderIdSet(orderIds)
                    .orderAmount(json.getDouble("split_total_amount"))
                    .ts(DateFormatUtil.toTs(json.getString("order_create_time"),true))
                    .build();
        });

        // 打印数据
        // skuUserOrderDS.print("skuUserOrderDS>>>>>>");


        // TODO 4.关联维表
        // 4.1 关联SKU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuDS = AsyncDataStream.unorderedWait(
                skuUserOrderDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        // System.out.println("=====" + input.getSkuId());
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            System.out.println("SKU_ID === " + dimInfo.getString("SKU_ID"));
                            input.setSpuId(dimInfo.getString("SPU_ID"));
                            input.setTrademarkId(dimInfo.getString("TM_ID"));
                            input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        // 打印数据
        // withSkuDS.print("withSkuDS>>>>>>");

        // 4.2 关联SPU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuDS = AsyncDataStream.unorderedWait(
                withSkuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setSpuName(dimInfo.getString("SPU_NAME"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        // 打印数据
        // withSpuDS.print("withSpuDS>>>>>>");

        // 4.3 关联TM
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setTrademarkName(dimInfo.getString("TM_NAME"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        // 打印数据
        // withTmDS.print("withTmDS>>>>>>");

        // 4.4 关联Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3DS = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        // System.out.println("input.getCategory3Id():" + input.getCategory3Id());
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory3Name(dimInfo.getString("NAME"));
                            input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        // 打印数据
        // withCategory3DS.print("withCategory3DS>>>>>>");


        // 4.5 关联Category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory2Name(dimInfo.getString("NAME"));
                            input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        // 打印数据
        // withCategory2DS.print("withCategory2DS>>>>>>");

        // 4.6 关联Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory1Name(dimInfo.getString("NAME"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        // 打印测试
        // withCategory1DS.print("withCategory1DS>>>>>>>>");


        // TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> tradeTrademarkCategoryUserSpuOrderWithWmDS = withCategory1DS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));


        // TODO 6.分组、开窗聚合
        // 6.1 分组
        KeyedStream<TradeTrademarkCategoryUserSpuOrderBean, String> keyedStream = tradeTrademarkCategoryUserSpuOrderWithWmDS.keyBy(new KeySelector<TradeTrademarkCategoryUserSpuOrderBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {
                return value.getUserId() + "-" +
                        value.getCategory1Id() + "-" +
                        value.getCategory1Name() + "-" +
                        value.getCategory2Id() + "-" +
                        value.getCategory2Name() + "-" +
                        value.getCategory3Id() + "-" +
                        value.getCategory3Name() + "-" +
                        value.getSpuId() + "-" +
                        value.getSpuName() + "-" +
                        value.getTrademarkId() + "-" +
                        value.getTrademarkName();
            }
        });

        // 6.2 开窗
        WindowedStream<TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 6.3 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> resultDS = windowedStream.reduce(new ReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
            @Override
            public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                return value1;
            }
        }, new WindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserSpuOrderBean> input, Collector<TradeTrademarkCategoryUserSpuOrderBean> out) throws Exception {

                // 获取数据
                TradeTrademarkCategoryUserSpuOrderBean orderBean = input.iterator().next();

                // 补充信息
                orderBean.setTs(System.currentTimeMillis());
                orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());

                // 输出数据
                out.collect(orderBean);
            }
        });


        // TODO 7.输出打印
        resultDS.print(">>>>>>");

        // TODO 8.将数据写出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_trade_trademark_category_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        // TODO 9.启动任务
        env.execute("DwsTradeTrademarkCategoryUserSpuOrderWindow");

    }
}
