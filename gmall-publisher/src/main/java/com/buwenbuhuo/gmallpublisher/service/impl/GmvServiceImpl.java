package com.buwenbuhuo.gmallpublisher.service.impl;

import com.buwenbuhuo.gmallpublisher.mapper.GmvMapper;
import com.buwenbuhuo.gmallpublisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Author 不温卜火
 * Create 2022-04-28 16:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:GMV模块Service层代码实现类
 */

@Service
public class GmvServiceImpl implements GmvService {
    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Double getGmv(int date) {
        return gmvMapper.selectGmv(date);
    }
}
