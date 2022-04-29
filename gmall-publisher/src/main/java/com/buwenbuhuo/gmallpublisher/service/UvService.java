package com.buwenbuhuo.gmallpublisher.service;

import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-29 9:03
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Uv模块Service层代码实现
 */
public interface UvService {

    // 获取按照渠道分组的日活数据
    Map getUvByCh(int date);

}
