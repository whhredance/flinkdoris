package com.whh.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: KeywordBean
 * Package: com.whh.gmall.realtime.bean
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 15:58
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;

    // 窗口闭合时间
    private String edt;

    // 关键词来源
    private String source;

    // 关键词
    private String keyword;

    // 当天日期
    private String cur_date;

    // 关键词出现频次
    private Long keyword_count;
}
