package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.UserLoginBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * Author 不温卜火
 * Create 2022-04-22 22:43
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:用户域用户登陆各窗口汇总表代码实现
 */
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取Kafka页面日志主题
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window_app";
        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, groupId));

        // TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = pageStringDS.map(JSON::parseObject);


        // TODO 4.过滤数据（uid != null & last_page_id = null）
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                JSONObject common = value.getJSONObject("common");
                JSONObject page = value.getJSONObject("page");
                return common.getString("uid") != null && page.getString("last_page_id") == null;
            }
        });


        // TODO 5.提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> filterWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        // TODO 6.按照uid分组
        KeyedStream<JSONObject, String> keyedStream = filterWithWmDS.keyBy(json -> json.getJSONObject("common").getString("uid"));


        // TODO 7.使用状态编程实现回流及独立活跃用户的提取
        SingleOutputStreamOperator<UserLoginBean> uvDS = keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            // 声明状态
            private ValueState<String> lastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<UserLoginBean> out) throws Exception {
                // 取出状态中的数据，即是上一次保存的日期
                String lastDt = lastVisitDt.value();

                // 获取当前数据中的时间并转换为日期
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                // 定义独立用户 & 回流用户数
                long uuCt = 0L;
                long backCt = 0L;

                // 状态保存的日期为null，则表示为新用户
                if (lastDt == null) {
                    uuCt = 1L;
                    lastVisitDt.update(curDt);
                } else {
                    // 状态保存的日期不为null，且与当前数据日期不同，则为今天第一条数据
                    if (!lastDt.equals(curDt)) {
                        uuCt = 1L;
                        lastVisitDt.update(curDt);

                        // 如果保存的日期与当前数据日期差值大于等于8，则为回流用户
                        Long lastTs = DateFormatUtil.toTs(lastDt);
                        long days = (ts - lastTs) / (1000L * 60 * 60 * 24);
                        if (days >= 8L) {
                            backCt = 1;
                        }
                    }
                }

                // 判断，如果当日独立用户数为"1",则输出
                if (uuCt == 1L) {
                    out.collect(new UserLoginBean("", "", backCt, uuCt, System.currentTimeMillis()));
                }
            }
        });


        // TODO 8.开窗、聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = uvDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        // 取出数据
                        UserLoginBean userLoginBean = values.iterator().next();

                        // 补充窗口信息
                        userLoginBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userLoginBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        // 输出数据
                        out.collect(userLoginBean);
                    }
                });

        // TODO 9.打印数据
        resultDS.print(">>>>>>");

        // TODO 10.将数据写出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        // TODO 11.启动任务
        env.execute("DwsUserUserLoginWindow");
    }
}
