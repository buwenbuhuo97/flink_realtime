package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-04-16 9:30
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: WaterMark测试代码实现工具类
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {

    private String id;
    private Double vc;
    private Long ts;
}
