package com.atguigu.bean;

/**
 * @author MengX
 * @create 2023/3/14 20:33:42
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 关键词
    private String keyword;

    // 关键词出现频次
    private Long keyword_count;

    // 时间戳
    private Long ts;
}
