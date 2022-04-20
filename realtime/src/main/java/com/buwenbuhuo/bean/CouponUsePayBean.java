package com.buwenbuhuo.bean;

import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-20 19:09
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:优惠券支付工具类
 */

@Data
public class CouponUsePayBean {
    /**
     * 字段解释：
     *      id          : 优惠券领用记录 id
     *      coupon_id   : 优惠券 id
     *      user_id     : 用户 id
     *      order_id    : 订单 id
     *      date_id     : 优惠券使用日期（支付）
     *      used_time   : 优惠券使用时间（支付）
     *      old         : 历史数据
     *      ts          : 时间戳
     */
    String id;
    String coupon_id;
    String user_id;
    String order_id;
    String date_id;
    String used_time;
    String old;
    String ts;
}
