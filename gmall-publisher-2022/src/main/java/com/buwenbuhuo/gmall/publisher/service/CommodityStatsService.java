package com.buwenbuhuo.gmall.publisher.service;

import com.buwenbuhuo.gmall.publisher.bean.CategoryCommodityStats;
import com.buwenbuhuo.gmall.publisher.bean.SpuCommodityStats;
import com.buwenbuhuo.gmall.publisher.bean.TrademarkCommodityStats;
import com.buwenbuhuo.gmall.publisher.bean.TrademarkOrderAmountPieGraph;

import java.util.List;

public interface CommodityStatsService {
    List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date);

    List<CategoryCommodityStats> getCategoryStatsService(Integer date);

    List<SpuCommodityStats> getSpuCommodityStats(Integer date);

    List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date);
}
