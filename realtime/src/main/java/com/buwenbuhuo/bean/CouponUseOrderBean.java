package com.buwenbuhuo.bean;

import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-20 19:02
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:优惠券下单实体类
 */

@Data
public class CouponUseOrderBean {
    /**
     * 字段解释：
     *      id          : 优惠券领用记录 id
     *      coupon_id   : 优惠券 id
     *      user_id     : 用户 id
     *      order_id    : 订单 id
     *      date_id     : 优惠券使用日期（下单）
     *      using_time   : 优惠券使用时间（下单）
     *      old         : 历史数据
     *      ts          : 时间戳
     */
    String id;
    String coupon_id;
    String user_id;
    String order_id;
    String date_id;
    String using_time;
    String old;
    String ts;
}

