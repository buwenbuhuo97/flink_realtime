package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.util.Set;

/**
 * Author 不温卜火
 * Create 2022-04-24 14:45
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域省份粒度下单实现类
 */

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindow {
    /**
     * 字段解释：
     *      stt                          : 窗口起始时间
     *      edt                          : 窗口结束时间
     *      provinceId                   ：省份 ID
     *      provinceName                 ：省份名称
     *      orderCount                   ：累计下单次数
     *      orderIdSet                   : 订单 ID 集合，用于统计下单次数
     *      orderAmount                  : 累计下单金额
     *      ts                           : 时间戳
     */
    String stt;
    String edt;
    String provinceId;

    @Builder.Default
    String provinceName = "";

    Long orderCount;

    @TransientSink
    Set<String> orderIdSet;

    Double orderAmount;
    Long ts;
}

