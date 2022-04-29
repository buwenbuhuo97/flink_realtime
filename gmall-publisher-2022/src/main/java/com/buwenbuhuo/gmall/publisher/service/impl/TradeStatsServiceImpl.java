package com.buwenbuhuo.gmall.publisher.service.impl;

import com.buwenbuhuo.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.buwenbuhuo.gmall.publisher.bean.TradeProvinceOrderCt;
import com.buwenbuhuo.gmall.publisher.bean.TradeStats;
import com.buwenbuhuo.gmall.publisher.mapper.TradeStatsMapper;
import com.buwenbuhuo.gmall.publisher.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TradeStatsServiceImpl implements TradeStatsService {

    @Autowired
    TradeStatsMapper tradeStatsMapper;

    @Override
    public Double getTotalAmount(Integer date) {
        return tradeStatsMapper.selectTotalAmount(date);
    }

    @Override
    public List<TradeStats> getTradeStats(Integer date) {
        return tradeStatsMapper.selectTradeStats(date);
    }

    @Override
    public List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date) {
        return tradeStatsMapper.selectTradeProvinceOrderCt(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date) {
        return tradeStatsMapper.selectTradeProvinceOrderAmount(date);
    }
}
