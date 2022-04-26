package com.buwenbuhuo.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * Author 不温卜火
 * Create 2022-04-26 12:25
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:模板方法设计模式模板接口
 */
public interface DimJoinFunction<T> {
    // 获取维度主键的方法
    String getKey(T input);

    void join(T input, JSONObject dimInfo);
}
