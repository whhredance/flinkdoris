package com.whh.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: TrafficHomeDetailPageViewBean
 * Package: com.whh.gmall.realtime.bean
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 16:42
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 首页独立访客数
    Long homeUvCt;

    // 商品详情页独立访客数
    Long goodDetailUvCt;
}
