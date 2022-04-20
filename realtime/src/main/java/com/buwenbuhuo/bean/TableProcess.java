package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-04-12 21:28
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: MySQL配置表工具类
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    /**
     *  字段解释：
     *      sourceTable  : 来源表
     *      sinkTable    : 输出表
     *      sinkColumns  : 输出字段
     *      sinkPk       : 主键字段
     *      sinkExtend   : 建表扩展
     */
    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkPk;
    String sinkExtend;

}
