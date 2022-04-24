package com.buwenbuhuo.app.dws;


import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.app.func.OrderDetailFilterFunction;
import com.buwenbuhuo.bean.TradeTrademarkCategoryUserSpuOrderBean;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-24 0:53
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域品牌-品类-用户-SPU粒度下单各窗口汇总表代码实现
 */
public class DwsTradeTrademarkCategoryUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.获取过滤后的OrderDetail表
        String groupId = "sku_user_order_window_app";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDs = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);

        // TODO 3.转换数据为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> skuUserOrderDS = orderDetailJsonObjDs.map(json -> TradeTrademarkCategoryUserSpuOrderBean.builder()
                .skuId(json.getString("sku_id"))
                .userId(json.getString("user_id"))
                .orderCount(1L)
                .orderAmount(json.getDouble("split_total_amount"))
                .build()
        );


        // TODO 4.关联维表



        // TODO 5.提取时间戳生成WaterMark



        // TODO 6.分组、开窗聚合



        // TODO 7.输出打印



        // TODO 8.将数据写出到ClickHouse



        // TODO 9.启动任务
        env.execute("DwsTradeTrademarkCategoryUserSpuOrderWindow");

    }
}
