package com.yixin.dw.flink.util;

import java.math.BigDecimal;
import java.util.HashMap;

/**
 * Create By 鸣宇淳 on 2020/2/17
 **/
public class StringHelper {
    public static String toStringNotNull(String source)
    {
        return source==null?"":source;
    }
    public static BigDecimal toBigDecimalNotNull(BigDecimal source)
    {
        return source==null?new BigDecimal(0):source;
    }
    public static Integer toBigIntegerNotNull(Integer source)
    {
        return source==null?new Integer(0):source;
    }
    public static <K,V> HashMap<K,V> toHashMapNotNull(HashMap<K,V> source)
    {
        return source==null?new HashMap<K,V>():source;
    }
}
