package com.buwenbuhuo.gmall.publisher.service;

import com.buwenbuhuo.gmall.publisher.bean.CouponReduceStats;

import java.util.List;

public interface CouponStatsService {
    List<CouponReduceStats> getCouponStats(Integer date);
}
