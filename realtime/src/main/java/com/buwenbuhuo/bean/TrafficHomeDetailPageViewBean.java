package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-22 21:44
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 流量域页面浏览实体类
 */

@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    /**
     * 字段解释：
     *      stt             : 窗口起始时间
     *      edt             : 窗口结束时间
     *      homeUvCt        : 首页独立访客数
     *      goodDetailUvCt  : 商品详情页独立访客数
     *      ts              : 时间戳
     */
    String stt;
    String edt;
    Long homeUvCt;
    Long goodDetailUvCt;
    Long ts;
}

