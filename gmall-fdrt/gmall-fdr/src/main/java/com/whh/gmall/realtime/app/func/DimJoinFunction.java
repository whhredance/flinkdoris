package com.whh.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: DimJoinFunction
 * Package: com.whh.gmall.realtime.app.func
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 17:11
 * @Version 1.0
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj) throws Exception;

    //获取维度主键的方法
    String getKey(T obj);
}
