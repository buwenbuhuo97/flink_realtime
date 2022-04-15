package com.buwenbuhuo.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.util.DateFormatUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Author 不温卜火
 * Create 2022-04-14 18:29
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:流量域独立访客事务事实表实现类
 * 数据流：
 * web/app -> Nginx -> 日志服务器（log） -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
 * 程 序：
 * Mock -> f1.sh -> kafka(ZK) -> BaseLogApp -> kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> kafka(ZK)
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生产环境设置为kafka主题的分区数
        env.setParallelism(1);

        // TODO 2.从 kafka dwd_traffic_page_log 主题中读取日志数据，封装为流
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil
                .getKafkaConsumer("dwd_traffic_page_log", "dwd_traffic_unique_visitor_detail_app");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);


        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // TODO 4.过滤 last_page_id 不为 null 的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        });

        // TODO 5.按照 Mid 分组
        KeyedStream<JSONObject, String> keyedByMidStream = filterDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        // TODO 6.使用状态编程进行每日登录数据去重
        SingleOutputStreamOperator<JSONObject> uvDetailDS = keyedByMidStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> visitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-dt", String.class);
                // 设置状态的TTL
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                // 初始化状态
                visitDtState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                // 取出状态以及当前数据的日期
                String dt = visitDtState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));

                // 如果状态数据为null或者状态日期与当前数据日期不同,则保留数据,同时更新状态,否则弃之
                if (dt == null || !dt.equals(curDt)) {
                    visitDtState.update(curDt);
                    return true;
                } else {
                    return false;
                }
            }
        });


        // TODO 7.将数据写入到Kafka  Kafka主题: dwd_traffic_unique_visitor_detail
        uvDetailDS.print(">>>>>>>>>>");
        uvDetailDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_unique_visitor_detail"));

        // TODO 8.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
