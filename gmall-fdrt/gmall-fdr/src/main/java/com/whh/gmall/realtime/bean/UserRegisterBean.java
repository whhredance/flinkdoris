package com.whh.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: UserRegisterBean
 * Package: com.whh.gmall.realtime.bean
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 16:47
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 注册用户数
    Long registerCt;
}
