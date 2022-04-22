package com.buwenbuhuo.bean;

import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-20 16:09
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:历史数据订单实体类
 */

@Data
public class OrderInfoRefundBean {
    /**
     *  字段解释：
     *      id           : 订单 id
     *      province_id  : 省份 id
     *      old          : 历史数据
     */
    String id;
    String province_id;
    String old;
}
