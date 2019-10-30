package com.longi.group.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @version 1.0
 * @CalssName CommonUtil
 * @Author sunke5
 * @Date 2019-10-23 17:32
 */
public class CommonUtil {
    public static String getHourMinuteByStr(String urFormat, String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse (dateStr);
        return new SimpleDateFormat (urFormat).format (date);
    }
}
