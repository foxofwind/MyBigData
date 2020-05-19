package com.yixin.hubg.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Create By 鸣宇淳 on 2020/2/12
 **/
public class DateTimeHelper {
    public static long DataStr2TimeStamp(String dataStr) {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        try {
            ts = Timestamp.valueOf(dataStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return ts.getTime();
    }


    public static String TimeStamp2String(Long timestamp) {
        String formats = "yyyy-MM-dd";
        String date = new SimpleDateFormat(formats, Locale.CHINA).format(new Date(timestamp));
        return date;
    }

}
