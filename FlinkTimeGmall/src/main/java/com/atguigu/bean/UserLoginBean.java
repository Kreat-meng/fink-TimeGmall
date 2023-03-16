package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author MengX
 * @create 2023/3/15 21:00:10
 */
@Data
@AllArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 回流用户数
    Long backCt;
    // 独立用户数
    Long uuCt;
    // 时间戳
    Long ts;
}
