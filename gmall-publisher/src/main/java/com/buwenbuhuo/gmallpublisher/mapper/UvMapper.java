package com.buwenbuhuo.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-29 8:53
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Uv模块Mapper层代码实现
 */
public interface UvMapper {

    /**
     * List可以用JavaBean替换。
     * 根据渠道分组，获取当日的日活数据
     * @param date 当天日期
     * @return
     *      lsit{
     *          Map[(ch -> Appstore),(uv -> 465),(uj -> 2),...],
     *          Map[(ch -> xiaomi),(uv -> 416),(uj -> 4),...],
     *          ......
     *      }
     */
    @Select("select ch,sum(uv_ct) uv,sum(uj_ct) uj from dws_traffic_channel_page_view_window where toYYYYMMDD(stt)=#{date} group by ch")
    List<Map> selectUvByCh(int date);

}
