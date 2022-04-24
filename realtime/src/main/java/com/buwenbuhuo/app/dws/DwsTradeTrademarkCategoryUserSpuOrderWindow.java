package com.buwenbuhuo.app.dws;

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

        // TODO 2.

        // TODO 3.转换数据结构

        // TODO 4.

        // TODO 5.

        // TODO 6.

        // TODO 7.

        // TODO 8.启动任务
        env.execute("DwsTradeTrademarkCategoryUserSpuOrderWindow");

    }
}
