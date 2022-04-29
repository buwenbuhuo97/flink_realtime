package com.buwenbuhuo.gmallpublisher.service.impl;

import com.buwenbuhuo.gmallpublisher.mapper.UvMapper;
import com.buwenbuhuo.gmallpublisher.service.UvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-29 9:04
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Uv模块Mapper层代码具体功能实现
 */

@Service
public class UvServiceImpl implements UvService {

    @Autowired
    private UvMapper uvMapper;

    @Override
    public Map getUvByCh(int date) {

        // 创建HashMap用来存放结果数据
        HashMap<String, BigInteger> resultMap = new HashMap<>();

        // 查询ClickHouse获取数据
        List<Map> mapList = uvMapper.selectUvByCh(date);

        // 遍历集合，取出渠道和日活数据放入结果集
        for (Map map : mapList) {
            resultMap.put((String) map.get("ch"), (BigInteger) map.get("uv"));
        }

        // 返回结果
        return resultMap;
    }
}
