package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-24 0:24
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域支付窗口实体类
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    /**
     * 字段解释：
     *      stt                          : 窗口起始时间
     *      edt                          : 窗口结束时间
     *      paymentSucUniqueUserCount    : 支付成功独立用户数
     *      paymentSucNewUserCount       : 支付成功新用户数
     *      ts                           : 时间戳
     */
    String stt;
    String edt;
    Long paymentSucUniqueUserCount;
    Long paymentSucNewUserCount;
    Long ts;
}

