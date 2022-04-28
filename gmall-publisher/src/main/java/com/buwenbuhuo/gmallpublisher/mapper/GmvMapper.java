package com.buwenbuhuo.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

/**
 * Author 不温卜火
 * Create 2022-04-28 16:09
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:GMV模块Mapper层代码实现
 */
public interface GmvMapper {

    // 查询ClickHouse，获取GMV总数
    @Select("select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    Double selectGmv(int date);

}
