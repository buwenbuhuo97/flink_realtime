package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-04-22 10:46
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:实体类 KeywordBean
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    /**
     * 各字段注解：
     *  stt             ：窗口起始时间
     *  edt             ：窗口闭合时间
     *  source          ：关键词来源
     *  keyword         ：关键词
     *  keyword_count   ：关键词出现频次
     *  ts              ：时间戳
     */
    private String stt;
    private String edt;
    private String source;
    private String keyword;
    private Long keyword_count;
    private Long ts;
}

