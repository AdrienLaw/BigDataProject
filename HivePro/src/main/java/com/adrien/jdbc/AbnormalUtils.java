package com.adrien.jdbc;

import java.util.HashMap;

/**
 * 异常处理
 */
public class AbnormalUtils {
    public static Object getAbnormal (Exception e) {
        Object abnormalType = e.getCause().getClass().toString();
        Object abnormalName = e.getCause().getMessage().toString();
        HashMap<String, Object> map = new HashMap<>();
        map.put("异常类型", abnormalType);
        map.put("异常点信息", abnormalName);
        return map.toString();

    }
}
