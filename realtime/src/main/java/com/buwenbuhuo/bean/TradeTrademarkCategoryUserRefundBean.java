package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Author 不温卜火
 * Create 2022-04-24 14:58
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易域品牌-品类-用户粒度退单实体类
 */
@Data
@AllArgsConstructor
@Builder
public class TradeTrademarkCategoryUserRefundBean {
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
     *      skuId                    : sku_id
     *      userId                   ：用户 ID
     *      refundCount              : 退单次数
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
    String skuId;

    String userId;
    Long refundCount;
    Long ts;

    public static void main(String[] args) {
        TradeTrademarkCategoryUserRefundBean build = builder().build();
        System.out.println(build);
    }
}

