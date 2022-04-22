package com.buwenbuhuo.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Author 不温卜火
 * Create 2022-04-22 10:46
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:TransientSink 注解
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}

