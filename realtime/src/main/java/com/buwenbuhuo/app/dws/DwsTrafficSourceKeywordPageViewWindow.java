package com.buwenbuhuo.app.dws;

import com.buwenbuhuo.app.func.SplitFunction;
import com.buwenbuhuo.bean.KeywordBean;
import com.buwenbuhuo.util.MyClickHouseUtil;
import com.buwenbuhuo.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-22 10:54
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:流量域来源关键词粒度页面浏览各窗口汇总表代码实现
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.使用DDL方式读取DWD层页面浏览日志创建表，同时获取事件事件生成watermark
        tableEnv.executeSql("" +
                "create table page_log( " +
                " `page` map<string,string>, " +
                " `ts` bigint, " +
                " `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                " WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "dws_traffic_source_keyword_pageview_window"));

        // TODO 3.过滤出搜索数据
        Table keyWordTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] key_word, " +
                "    rt " +
                "from " +
                "    page_log " +
                "where page['item'] is not null " +
                "and page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("key_word_table", keyWordTable);

        // TODO 4.使用自定义函数分词处理
        // 4.1 注册外卖
        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);
        // 4.2 数据处理
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word, " +
                "    rt " +
                "FROM key_word_table, LATERAL TABLE(SplitFunction(key_word))");
        tableEnv.createTemporaryView("split_table", splitTable);


        // TODO 5.分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP() ts " +
                "from " +
                "    split_table " +
                "group by TUMBLE(rt, INTERVAL '10' SECOND),word");

        // TODO 6.将数据转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>");

        // TODO 7.将数据写出到ClickHouse
        keywordBeanDataStream.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        // TODO 8.启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
