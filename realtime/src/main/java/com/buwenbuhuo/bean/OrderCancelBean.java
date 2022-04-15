package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-04-14 23:24
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelBean {
    /**
     * id:          订单 id
     * user_id:     用户 id
     * province_id: 省份 id
     * cancel_time: 取消订单时间
     * old:Maxwell  采集的历史数据字段 old
     * ts:          时间戳
     */
    String id;
    String user_id;
    String province_id;
    String cancel_time;
    String old;
    String ts;
}
