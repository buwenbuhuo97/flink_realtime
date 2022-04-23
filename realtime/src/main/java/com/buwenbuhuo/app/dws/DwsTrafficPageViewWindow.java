package com.buwenbuhuo.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.TrafficHomeDetailPageViewBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * Author 不温卜火
 * Create 2022-04-22 21:46
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:流量域页面浏览各窗口汇总表代码实现
 */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.读取Kafka页面日志主题数据创建流
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window_app";
        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, groupId));


        // TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = pageStringDS.map(JSON::parseObject);


        // TODO 4.过滤数据，只需要访问主页跟商品详情页的数据
        SingleOutputStreamOperator<JSONObject> homeAndDetailDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                return "good_detail".equals(pageId) || "home".equals(pageId);
            }
        });

        // 使用flatMap代替
        /*
        SingleOutputStreamOperator<JSONObject> homeAndDetailDS = pageStringDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("good_detail".equals(pageId) || "home".equals(pageId)) {
                    out.collect(jsonObject);
                }
            }
        });
        */

        // TODO 5.提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> homeAndDetailPageWithDS = homeAndDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));


        // TODO 6.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = homeAndDetailPageWithDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        // TODO 7.使用状态编程计算主页及商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

            // 定义两个状态
            private ValueState<String> homeLastVisitDt;
            private ValueState<String> detailLastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 设置状态TTL
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                // 初始化Home页面访问日期状态
                ValueStateDescriptor<String> homeDtDescriptor = new ValueStateDescriptor<>("home-dt", String.class);
                homeDtDescriptor.enableTimeToLive(stateTtlConfig);
                homeLastVisitDt = getRuntimeContext().getState(homeDtDescriptor);

                // 初始化商品详情页访问日期状态
                ValueStateDescriptor<String> detailDtDescriptor = new ValueStateDescriptor<>("detail-dt", String.class);
                detailDtDescriptor.enableTimeToLive(stateTtlConfig);
                detailLastVisitDt = getRuntimeContext().getState(detailDtDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                // 获取当前页面信息及时间戳并将其转换为日期
                String pageId = value.getJSONObject("page").getString("page_id");
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                // 定义主页以及商品详情页的访问次数
                long homeUvCt = 0L;
                long detailUvCt = 0L;

                // 判断是否为主页数据
                if ("home".equals(pageId)) {
                    // 判断状态以及与当前日期是否相同
                    String homeLastDt = homeLastVisitDt.value();
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeUvCt = 1L;
                        homeLastVisitDt.update(curDt);
                    }
                } else { // 商品详情页
                    // 判断状态以及与当前日期是否相同
                    String detailLastDt = detailLastVisitDt.value();
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailUvCt = 1L;
                        detailLastVisitDt.update(curDt);
                    }
                }

                // 封装JavaBean并写出数据
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "",
                            homeUvCt,
                            detailUvCt,
                            System.currentTimeMillis()));
                }
            }
        });


        // TODO 8.开窗、聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        // 获取数据
                        TrafficHomeDetailPageViewBean pageViewBean = values.iterator().next();

                        // 设置窗口信息
                        pageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        pageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        // 输出数据
                        out.collect(pageViewBean);
                    }
                });

        // TODO 9. 测试打印
        resultDS.print(">>>>>>>");

        // TODO 10.将数据输出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        // TODO 11.启动任务
        env.execute("DwsTrafficPageViewWindow");
    }
}
