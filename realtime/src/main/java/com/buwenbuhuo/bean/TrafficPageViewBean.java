package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-22 16:25
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:流量域版本-渠道-地区-访客类别粒度页面浏览实体类
 */
@Data
@AllArgsConstructor
public class TrafficPageViewBean {
    /**
     * 字段解释：
     *      stt             : 窗口起始时间
     *      edt             : 窗口结束时间
     *      vc              : app 版本号
     *      ch              : 渠道
     *      ar              : 地区
     *      isNew           : 新老访客状态标记
     *      uvCt            : 独立访客数
     *      svCt            : 会话数
     *      pvCt            : 页面浏览数
     *      durSum          : 累计访问时长
     *      ujCt            : 跳出会话数
     *      ts              : 时间戳
     */
    String stt;
    String edt;
    String vc;
    String ch;
    String ar;
    String isNew ;
    Long uvCt;
    Long svCt;
    Long pvCt;
    Long durSum;
    Long ujCt;
    Long ts;
}

