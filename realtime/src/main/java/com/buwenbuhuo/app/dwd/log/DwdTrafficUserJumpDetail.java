package com.buwenbuhuo.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-14 19:06
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 流量域用户跳出事务事实表实现类
 * 数据流：
 * web/app -> Nginx -> 日志服务器（log） -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
 * 程 序：
 * Mock -> f1.sh -> kafka(ZK) -> BaseLogApp -> kafka(ZK) -> DwdTrafficUserJumpDetail -> kafka(ZK)
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生产环境设置为kafka主题的分区数
        env.setParallelism(1);

        // TODO 2.从 kafka dwd_traffic_page_log 主题读取日志数据，封装为流
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil
                .getKafkaConsumer("dwd_traffic_page_log", "dwd_traffic_user_jump_detail_app");
        DataStreamSource<String> pageLog = env.addSource(kafkaConsumer);

        // TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> mappedStream = pageLog.map(JSON::parseObject);

        // TODO 4.提取事件事件生成WaterMark，用于用户跳出统计
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = mappedStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })
        );

        // TODO 5.按照 mid 分组
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjWithWmDS.keyBy(json -> json
                .getJSONObject("common")
                .getString("mid"));

        // TODO 6.定义模式序列
        // 写法1
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10L));

        // 另一种写法
        /*Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        })       // 默认为宽松近邻，需要指定为严格模式才行
                .times(2)
                .consecutive()
                .within(Time.seconds(10L));
        */


        // TODO 7.把模式序列应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedByMidStream, pattern);

        // TODO 8.提取匹配上的事件以及超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time-out") {
        };

        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long timeoutTimestamp) throws Exception {
                return map.get("first").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("first").get(0);
            }
        });


        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        // TODO 9.合并两个事件流
        selectDS.print("Select>>>>>>");
        timeOutDS.print("TimeOut>>>>>>");
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);


        // TODO 10.将数据写出到 Kafka
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_user_jump_detail"));


        // TODO 11.启动
        env.execute("DwdTrafficUserJumpDetail");
    }
}
