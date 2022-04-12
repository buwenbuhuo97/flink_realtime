package com.buwenbuhuo.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Properties;

/**
 * Author 不温卜火
 * Create 2022-04-12 20:49
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:KafkaUtil工具类：接收Kafka数据，过滤空值数据
 */
public class MyKafkaUtil {
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String group_id){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop01:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);

        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            // 核心方法
            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record == null || record.value() == null){
                    return "";
                }else {
                    return new String(record.value());
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        },properties);


    }
}
