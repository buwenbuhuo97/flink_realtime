package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-23 23:59
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域加购商品
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    /**
     * 字段解释：
     *      stt             : 窗口起始时间
     *      edt             : 窗口结束时间
     *      cartAddUuCt     : 加购独立用户数
     *      ts              : 时间戳
     */
    String stt;
    String edt;
    Long cartAddUuCt;
    Long ts;
}

