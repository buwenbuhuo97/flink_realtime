package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.util.Set;

/**
 * Author 不温卜火
 * Create 2022-04-24 0:48
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:DWS 层交易域品牌-品类-用户-SPU粒度下单各窗口实体类
 */
@Data
@AllArgsConstructor
@Builder
public class TradeTrademarkCategoryUserSpuOrderBean {
    /**
     * 字段解释：
     *      stt                      : 窗口起始时间
     *      edt                      : 窗口结束时间
     *      trademarkId              : 品牌 ID
     *      trademarkName            : 品牌名称
     *      category1Id              : 一级品类 ID
     *      category1Name            : 一级品类名称
     *      category2Id              : 二级品类 ID
     *      category2Name            : 二级品类名称
     *      category3Id              : 三级品类 ID
     *      category3Name            : 三级品类名称
     *      orderIdSet               : 订单 ID
     *      skuId                    : sku_id
     *      spuId                    : spu_id
     *      orderIdSet               : 订单 ID
     *      spuName                  : spu 名称
     *      orderCount               : 下单次数
     *      orderAmount              : 下单金额
     *      ts                       : 时间戳
     */
    String stt;
    String edt;
    String trademarkId;
    String trademarkName;
    String category1Id;
    String category1Name;
    String category2Id;
    String category2Name;
    String category3Id;
    String category3Name;
    @TransientSink
    Set<String> orderIdSet;
    @TransientSink
    String skuId;
    String userId;
    String spuId;
    String spuName;
    Long orderCount;
    Double orderAmount;
    Long ts;

    public static void main(String[] args) {
        TradeTrademarkCategoryUserSpuOrderBean build = builder().build();
        System.out.println(build);
    }
}

