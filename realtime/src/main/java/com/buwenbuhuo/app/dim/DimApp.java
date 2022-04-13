package com.buwenbuhuo.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.app.func.DimSinkFunction;
import com.buwenbuhuo.app.func.TableProcessFunction;
import com.buwenbuhuo.bean.TableProcess;
import com.buwenbuhuo.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author 不温卜火
 * Create 2022-04-12 20:36
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: Dim主程序入口
 * 数据流；
 *  web/app -> Nginx -> 业务服务器 -> MySQL(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Phoenix(DIM)
 * 程序：
 *  Mock -> MySQL(Binlog) -> mxw.sh -> kafka(ZK) -> DimApp -> Phoenix(HBase HDFS/ZK)
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生产环境设置为kafka主题的分区数
        env.setParallelism(1);

        // 可能需要的配置
        /*env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop01:8020/xxx/xx");*/

        // TODO 2.读取Kafka topic_db主题数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.
                getKafkaConsumer("topic_db", "dim_app"));

        // TODO 3.过滤掉非JSON格式的数据，并将其写入侧输出流
        OutputTag<String> dirtyDataTag = new OutputTag<String>("Dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyDataTag, s);
                }
            }
        });

        // 读取脏数据并打印
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyDataTag);
        sideOutput.print("Dirty>>>>>>>>>>");


        // TODO 4.使用FlinkCDC读取MySQL中的配置信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop01")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlSouceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),
                "MysqlSouce");


        // TODO 5.将配置信息流处理成广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state",
                String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = mysqlSouceDS.broadcast(mapStateDescriptor);

        // TODO 6.链接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        // TODO 7.广播流数据处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));


        // TODO 8.将数据写出到Phoenix中
        // hbaseDS.print(">>>>>>>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());


        // TODO 9.启动任务
        env.execute("dim-app");
    }
}
