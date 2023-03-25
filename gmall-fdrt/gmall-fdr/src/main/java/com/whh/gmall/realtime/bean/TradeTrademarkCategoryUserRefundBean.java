package com.whh.gmall.realtime.bean;

import com.whh.gmall.realtime.app.func.TransientSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

/**
 * ClassName: TradeTrademarkCategoryUserRefundBean
 * Package: com.whh.gmall.realtime.bean
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 17:20
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@Builder
public class TradeTrademarkCategoryUserRefundBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;

    // 订单 ID
    @TransientSink
    Set<String> orderIdSet;

    // sku_id
    @TransientSink
    String skuId;

    // 用户 ID
    String userId;
    // 当天日期
    String curDate;

    // 退单次数
    Long refundCount;
    // 时间戳
    @TransientSink
    Long ts;
}
