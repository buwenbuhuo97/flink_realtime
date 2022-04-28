package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.app.func.DimAsyncFunction;
import com.buwenbuhuo.bean.TradeTrademarkCategoryUserRefundBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

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

        // TODO 2.从 Kafka dwd_trade_order_refund 主题读取退单明细数据并创建流
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3. 转换为JSON对象，然后过滤去重数据（使用状态编程的方式取第一条数据）
        // 3.1 转换JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (!"".equals(value)) {
                    out.collect(JSON.parseObject(value));
                }
            }
        });

        // 3.2 过滤去重数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.keyBy(json -> json.getString("id"))
                .filter(new RichFilterFunction<JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .build();
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        //取出状态数据
                        String state = valueState.value();

                        if (state == null) {
                            valueState.update("1");
                            return true;
                        } else {
                            return false;
                        }
                    }
                });


        // TODO 4.将每行数据转换为JavaBean对象
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTrademarkDS = filterDS.map(json ->
                TradeTrademarkCategoryUserRefundBean
                        .builder()
                        .skuId(json.getString("sku_id"))
                        .userId(json.getString("user_id"))
                        .refundCount(1L)
                        .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                        .build());

        // 测试打印
        // tradeTrademarkDS.print("tradeTrademarkDS>>>>>>");

        // TODO 5.关联维表信息
        // 5.1 关联SKU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                tradeTrademarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 测试打印
        // withSkuInfoDS.print("withSkuInfoDS>>>>>>");

        // 5.2 关联品牌表 base_trademark
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
        // withTrademarkDS.print("withTrademarkDS>>>>>>");

        // 5.3 关联Category3
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
        // withCategory3DS.print("withCategory3DS>>>>>>");


        // 5.4 关联Category2
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
        // withCategory2DS.print("withCategory2DS>>>>>>");


        // 5.5 关联Category1
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
        // withCategory1DS.print("withCategory1DS>>>>>>");


        // TODO 6.设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withWaterMarkDS = withCategory1DS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));


        // TODO 7.分组、开窗、聚合
        // 7.1 分组
        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> keyedAggDS = withWaterMarkDS.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                return value.getTrademarkId() + "-" +
                        value.getTrademarkName() + "-" +
                        value.getTrademarkId() + "-" +
                        value.getCategory1Name() + "-" +
                        value.getCategory2Id() + "-" +
                        value.getCategory2Name() + "-" +
                        value.getCategory3Id() + "-" +
                        value.getCategory3Name() + "-" +
                        value.getUserId();
            }
        });

        // 7.2 开窗
        WindowedStream<TradeTrademarkCategoryUserRefundBean, String, TimeWindow> windowDS = keyedAggDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 7.3 聚合
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
                TradeTrademarkCategoryUserRefundBean refundBean = input.iterator().next();

                // 补充信息
                refundBean.setTs(System.currentTimeMillis());
                refundBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                refundBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                // 输出数据
                out.collect(refundBean);
            }
        });

        // TODO 8.输出打印
        resultDS.print(">>>>>>");

        // TODO 9.将数据写出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        // TODO 10.启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");

    }
}
