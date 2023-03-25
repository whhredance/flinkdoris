package com.whh.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: TradePaymentWindowBean
 * Package: com.whh.gmall.realtime.bean
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 16:52
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 当天日期
    String curDate;

    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;

    // 支付成功新用户数
    Long paymentSucNewUserCount;
}
