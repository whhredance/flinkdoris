package com.whh.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: CartAddUuBean
 * Package: com.whh.gmall.realtime.bean
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 16:49
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 当天日期
    String curDate;

    // 加购独立用户数
    Long cartAddUuCt;
}
