package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.UserRegisterBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * Create 2022-04-22 23:59
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:用户域用户注册各窗口汇总表代码实现
 */
public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取用户注册主题数据
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window_app";
        DataStreamSource<String> registerDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3.将每行数据转换为JavaBean对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = registerDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return new UserRegisterBean("", "", 1L, jsonObject.getLong("ts") * 1000L);
        });

        // TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        // TODO 5.开窗、聚合
        AllWindowedStream<UserRegisterBean, TimeWindow> windowedStream = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<UserRegisterBean> resultDS = windowedStream.reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                return value1;
            }
        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                //提取数据
                UserRegisterBean registerBean = values.iterator().next();

                //补充窗口信息
                registerBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                registerBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                //输出数据
                out.collect(registerBean);
            }
        });

        // TODO 6.测试打印
        resultDS.print(">>>>>>");

        // TODO 7.将数据写出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_user_user_register_window values(?,?,?,?)"));

        // TODO 8.启动任务
        env.execute("DwsUserUserRegisterWindow");
    }
}
