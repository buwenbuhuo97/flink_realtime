package com.buwenbuhuo.gmall.publisher.service;

import com.buwenbuhuo.gmall.publisher.bean.UserChangeCtPerType;
import com.buwenbuhuo.gmall.publisher.bean.UserPageCt;
import com.buwenbuhuo.gmall.publisher.bean.UserTradeCt;

import java.util.List;

public interface UserStatsService {
    List<UserPageCt> getUvByPage(Integer date);

    List<UserChangeCtPerType> getUserChangeCt(Integer date);

    List<UserTradeCt> getTradeUserCt(Integer date);
}
