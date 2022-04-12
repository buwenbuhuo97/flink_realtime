package com.buwenbuhuo.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.TableProcess;
import com.buwenbuhuo.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-12 21:40
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 处理主流和广播流
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> stateDescriptor;
    private Connection connection;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 广播流Value:
     * {
     * "before":null,"after":{"source_table":1,"sink_table":"三星","sink_columns":"/static/default.jpg"....},
     * "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1649744439676,
     * "snapshot":"false","db":"gmall-210927-flink","sequence":null,"table":"base_trademark",
     * "server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
     * "op":"r","ts_ms":1649744439678,"transaction":null
     * }
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 1.获取并解析数据为JavaBean对象
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // 2.校验表是否存在，如果不存在则建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        // 3.将数据写入状态
        String key = tableProcess.getSourceTable();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        broadcastState.put(key, tableProcess);

    }

    /**
     * 在Phoenix中校验并建表
     * 建表语句：create table if not exists db.tn(id varchar primary key,name varchar,...) xxx
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;
        try {
            // 处理字段
            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                // 获取字段
                String column = columns[i];

                // 判断是否为主键字段
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }

                // 不是最后一个字段，则添加","
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }
            sql.append(")").append(sinkExtend);
            System.out.println(sql);

            // 预编译sql
            preparedStatement = connection.prepareStatement(sql.toString());
            // 执行
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("建表" + sinkTable + "失败！");
        } finally {
            // 资源释放
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 主流Value:
     * {
     * "database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,
     * "data":{"id":100924,"user_id":"93","sku_id":16,"cart_price":4488.00,"sku_num":1,"img_url":"http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-sklaALrngAAHGDqdpFtU741.jpg",
     * "sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 8GB+128GB亮黑色全网通5G手机",
     * "is_checked":null,"create_time":"2020-06-14 09:28:57",
     * "operate_time":null,"is_ordered":1,
     * "order_time":"2021-10-17 09:28:58",
     * "source_type":"2401","source_id":null},
     * "old":{"is_ordered":0,"order_time":null}
     * }
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));

        String type = value.getString("type");

        if (tableProcess != null && ("bootstrap-insert".equals(type) || "insert".equals(type) || "update".equals(type))) {
            // 2.根据sinkColumns配置信息过滤字段
            filter(value.getJSONObject("data"), tableProcess.getSinkColumns());

            // 3.补充sinkTable字段写出
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);
        } else {
            System.out.println("过滤掉：" + value);
        }

    }


    /**
     * @param data        {"di":"11","tn_name":"hadoop","url":"xxxx"}
     * @param sinkColumns id,tn_name
     *                    最终结果：            {"di":"11","tn_name":"hadoop"}
     */
    private void filter(JSONObject data, String sinkColumns) {
        String[] split = sinkColumns.split(",");
        List<String> columsList = Arrays.asList(split);

        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columsList.contains(next.getKey())) {
                iterator.remove();
            }
        }

        // 简写
        // data.entrySet().removeIf(next -> !columsList.contains(next.getKey()));

    }
}
