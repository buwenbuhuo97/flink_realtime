package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-24 0:12
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域下单窗口实体类
 */
@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    /**
     * 字段解释：
     *      stt                         : 窗口起始时间
     *      edt                         : 窗口结束时间
     *      orderUniqueUserCount        : 下单独立用户数
     *      orderNewUserCount           : 下单新用户数
     *      orderActivityReduceAmount   : 下单活动减免金额
     *      orderCouponReduceAmount     : 下单优惠券减免金额
     *      orderOriginalTotalAmount    : 下单原始金额
     *      ts                          : 时间戳
     */
    String stt;
    String edt;
    Long orderUniqueUserCount;
    Long orderNewUserCount;
    Double orderActivityReduceAmount;
    Double orderCouponReduceAmount;
    Double orderOriginalTotalAmount;
    Long ts;
}

