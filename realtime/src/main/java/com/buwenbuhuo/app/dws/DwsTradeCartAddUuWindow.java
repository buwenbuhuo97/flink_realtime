package com.buwenbuhuo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.CartAddUuBean;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
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
 * Create 2022-04-24 0:01
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域加购各窗口汇总表代码实现
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.从 Kafka dwd_trade_cart_add 主题读取数据 读取流
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window_app";
        DataStreamSource<String> cartAddStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3.将每行数据转换为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = cartAddStringDS.map(JSON::parseObject);


        // TODO 4.提取时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                // yyyy-MM-dd HH:mm:ss
                String createTime = element.getString("create_time");
                return DateFormatUtil.toTs(createTime, true);
            }
        }));

        // TODO 5.按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));


        // TODO 6.过滤出独立用户，同时转换数据结构
        SingleOutputStreamOperator<CartAddUuBean> cartAddUuDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartAddDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cat_add", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);

                lastCartAddDt = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                // 提取状态编程
                String lastDt = lastCartAddDt.value();
                // yyyy-MM-dd HH:mm:ss
                String createTime = value.getString("create_time");
                String curDt = createTime.split(" ")[0];

                // 如果状态数据为null 或者与 当前日期不是同一天，则保留数据，更新状态
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddDt.update(curDt);
                    out.collect(new CartAddUuBean("", "", 1L, 0L));

                }
            }
        });


        // TODO 7.开窗、聚合
        AllWindowedStream<CartAddUuBean, TimeWindow> windowedStream = cartAddUuDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<CartAddUuBean> resultDS = windowedStream.reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                // 取出数据
                CartAddUuBean cartAddUuBean = values.iterator().next();

                // 补充窗口信息
                cartAddUuBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                cartAddUuBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                // 补充版本信息
                cartAddUuBean.setTs(System.currentTimeMillis());

                // 输出数据
                out.collect(cartAddUuBean);
            }
        });

        // TODO 8.测试打印
        resultDS.print(">>>>>>");

        // TODO 9. 将数据写出到ClickHouse
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        // TODO 10.启动任务
        env.execute("DwsTradeCartAddUuWindow");

    }
}
