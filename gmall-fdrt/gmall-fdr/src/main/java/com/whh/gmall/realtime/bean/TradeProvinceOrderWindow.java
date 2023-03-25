package com.whh.gmall.realtime.bean;

import com.whh.gmall.realtime.app.func.TransientSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * ClassName: TradeProvinceOrderWindow
 * Package: com.whh.gmall.realtime.bean
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 17:17
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindow {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 省份 ID
    String provinceId;

    // 省份名称
    @Builder.Default
    String provinceName = "";

    // 当天日期
    String curDate;

    // 累计下单次数
    Long orderCount;

    // 累计下单金额
    BigDecimal orderAmount;

    // 时间戳
    @TransientSink
    Long ts;
}
