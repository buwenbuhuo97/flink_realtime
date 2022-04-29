package com.buwenbuhuo.gmall.publisher.service;

import com.buwenbuhuo.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.buwenbuhuo.gmall.publisher.bean.TradeProvinceOrderCt;
import com.buwenbuhuo.gmall.publisher.bean.TradeStats;

import java.util.List;

public interface TradeStatsService {
    Double getTotalAmount(Integer date);

    List<TradeStats> getTradeStats(Integer date);

    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);
}
