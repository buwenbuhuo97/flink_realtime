package com.buwenbuhuo.common;

/**
 * Author 不温卜火
 * Create 2022-04-22 10:43
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:常量类 GmallConstant
 */
public class GmallConstant {
    /**
     * 10 单据状态
     *      ORDER_STATUS_UNPAID     ：未支付
     *      ORDER_STATUS_PAID       ：已支付
     *      ORDER_STATUS_CANCEL     ：已取消
     *      ORDER_STATUS_FINISH     ：已完成
     *      ORDER_STATUS_REFUND     ：退款中
     *      ORDER_STATUS_REFUND_DONE：退款完成
     */
    public static final String ORDER_STATUS_UNPAID="1001";
    public static final String ORDER_STATUS_PAID="1002";
    public static final String ORDER_STATUS_CANCEL="1003";
    public static final String ORDER_STATUS_FINISH="1004";
    public static final String ORDER_STATUS_REFUND="1005";
    public static final String ORDER_STATUS_REFUND_DONE="1006";

    /**
     * 11 支付状态
     *      PAYMENT_TYPE_ALIPAY    ：支付宝
     *      PAYMENT_TYPE_WECHAT    ：微信
     *      PAYMENT_TYPE_UNION     ：银联
     */
    public static final String PAYMENT_TYPE_ALIPAY="1101";
    public static final String PAYMENT_TYPE_WECHAT="1102";
    public static final String PAYMENT_TYPE_UNION="1103";

    /**
     * 12 评价
     *      APPRAISE_GOOD    ：好评
     *      APPRAISE_SOSO    ：中评
     *      APPRAISE_BAD     ：差评
     *      APPRAISE_AUTO    ：自动
     */
    public static final String APPRAISE_GOOD="1201";
    public static final String APPRAISE_SOSO="1202";
    public static final String APPRAISE_BAD="1203";
    public static final String APPRAISE_AUTO="1204";

    /**
     * 13 退货原因
     *      REFUND_REASON_BAD_GOODS      ：质量问题
     *      REFUND_REASON_WRONG_DESC     ：商品描述与实际描述不一致
     *      REFUND_REASON_SALE_OUT       ：缺货
     *      REFUND_REASON_SIZE_ISSUE     ：号码不合适
     *      REFUND_REASON_MISTAKE        ：拍错
     *      REFUND_REASON_NO_REASON      ：不想买了
     *      REFUND_REASON_OTHER          ：其他
     */
    public static final String REFUND_REASON_BAD_GOODS="1301";
    public static final String REFUND_REASON_WRONG_DESC="1302";
    public static final String REFUND_REASON_SALE_OUT="1303";
    public static final String REFUND_REASON_SIZE_ISSUE="1304";
    public static final String REFUND_REASON_MISTAKE="1305";
    public static final String REFUND_REASON_NO_REASON="1306";
    public static final String REFUND_REASON_OTHER="1307";

    /**
     * 14 购物券状态
     *      COUPON_STATUS_UNUSED     ：未使用
     *      COUPON_STATUS_USING      ：使用中
     *      COUPON_STATUS_USED       ：已使用
     */
    public static final String COUPON_STATUS_UNUSED="1401";
    public static final String COUPON_STATUS_USING="1402";
    public static final String COUPON_STATUS_USED="1403";

    /**
     * 15 退款类型
     *      REFUND_TYPE_ONLY_MONEY      ：仅退款
     *      REFUND_TYPE_WITH_GOODS      ：退货退款
     */
    public static final String REFUND_TYPE_ONLY_MONEY="1501";//
    public static final String REFUND_TYPE_WITH_GOODS="1502";//

    /**
     * 24 来源类型
     *      SOURCE_TYPE_QUREY              ：用户查询
     *      SOURCE_TYPE_PROMOTION          ：商品推广
     *      SOURCE_TYPE_AUTO_RECOMMEND     ：智能推荐
     *      SOURCE_TYPE_ACTIVITY           ：促销活动
     */
    public static final String SOURCE_TYPE_QUREY="2401";
    public static final String SOURCE_TYPE_PROMOTION="2402";
    public static final String SOURCE_TYPE_AUTO_RECOMMEND="2403";
    public static final String SOURCE_TYPE_ACTIVITY="2404";

    /**
     * 33 购物券范围
     *      COUPON_RANGE_TYPE_CATEGORY3       ：
     *      COUPON_RANGE_TYPE_TRADEMARK       ：
     *      COUPON_RANGE_TYPE_SPU             ：
     */
    public static final String COUPON_RANGE_TYPE_CATEGORY3="3301";
    public static final String COUPON_RANGE_TYPE_TRADEMARK="3302";
    public static final String COUPON_RANGE_TYPE_SPU="3303";

    /**
     * 32 购物券类型
     *      COUPON_TYPE_MJ       ：满减
     *      COUPON_TYPE_DZ       ：满量打折
     *      COUPON_TYPE_DJ       ：代金券
     */
    public static final String COUPON_TYPE_MJ="3201";
    public static final String COUPON_TYPE_DZ="3202";
    public static final String COUPON_TYPE_DJ="3203";

    /**
     * 31 购物券范围
     *      ACTIVITY_RULE_TYPE_MJ       ：
     *      ACTIVITY_RULE_TYPE_DZ       ：
     *      ACTIVITY_RULE_TYPE_ZK       ：
     */
    public static final String ACTIVITY_RULE_TYPE_MJ="3101";
    public static final String ACTIVITY_RULE_TYPE_DZ ="3102";
    public static final String ACTIVITY_RULE_TYPE_ZK="3103";

    /**
     *
     *      KEYWORD_SEARCH       ：
     *      KEYWORD_CLICK        ：
     *      KEYWORD_CART         ：
     *      KEYWORD_ORDER        ：
     */
    public static final String KEYWORD_SEARCH="SEARCH";
    public static final String KEYWORD_CLICK="CLICK";
    public static final String KEYWORD_CART="CART";
    public static final String KEYWORD_ORDER="ORDER";

}
