package com.buwenbuhuo.app;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author 不温卜火
 * Create 2022-04-16 9:28
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:WaterMark测试代码2
 */
public class WmTest02 {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生产环境设置为kafka主题的分区数
        env.setParallelism(3);

        // TODO 2.核心部分
        // 1001,23,5,1234
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop01", 7777);

        // 转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(
                    split[0],
                    Double.parseDouble(split[1]),
                    Long.parseLong(split[2]));
        });

        // 提取时间戳
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        // 分组开窗聚合
        SingleOutputStreamOperator<WaterSensor> result = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc");

        // 打印
        result.print();

        // 启动
        env.execute();

    }
}
