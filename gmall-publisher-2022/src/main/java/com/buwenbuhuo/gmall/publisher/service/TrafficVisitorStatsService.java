package com.buwenbuhuo.gmall.publisher.service;

import com.buwenbuhuo.gmall.publisher.bean.TrafficVisitorStatsPerHour;
import com.buwenbuhuo.gmall.publisher.bean.TrafficVisitorTypeStats;

import java.util.List;

public interface TrafficVisitorStatsService {
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);

    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);
}
