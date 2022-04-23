package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-22 23:57
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:用户注册实体类
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    /**
     * 字段解释：
     *      stt             : 窗口起始时间
     *      edt             : 窗口结束时间
     *      registerCt      : 注册用户数
     *      ts              : 时间戳
     */
    String stt;
    String edt;
    Long registerCt;
    Long ts;
}

