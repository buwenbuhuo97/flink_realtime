package com.buwenbuhuo.gmall.publisher.service;

import com.buwenbuhuo.gmall.publisher.bean.TrafficKeywords;

import java.util.List;

public interface TrafficKeywordsService {
    List<TrafficKeywords> getKeywords(Integer date);
}
