package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-22 22:41
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 用户登录窗口实现类
 */

@Data
@AllArgsConstructor
public class UserLoginBean {
    /**
     * 字段解释：
     *      stt             : 窗口起始时间
     *      edt             : 窗口结束时间
     *      backCt          : 回流用户数
     *      uuCt            : 独立用户数
     *      ts              : 时间戳
     */
    String stt;
    String edt;
    Long backCt;
    Long uuCt;
    Long ts;
}

