package com.buwenbuhuo.app;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Objects;
import java.util.Properties;

/**
 * Author 不温卜火
 * Create 2022-04-18 19:09
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: Kafka不处理null值代码实现
 */
public class KafkaConsumerTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>("test",
                new SimpleStringSchema(),
                properties));

        SingleOutputStreamOperator<String> filter = streamSource.filter(Objects::nonNull);

        filter.print();

        env.execute();

    }
}
