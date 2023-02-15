package com.myykk.utility;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Util {
    public static String formatDateTo(Date d, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(d);
    }

    public static String getYMD(int digits) {

        Calendar myCalendar = Calendar.getInstance();
        StringBuffer sb = new StringBuffer();
        int year = myCalendar.get(Calendar.YEAR);
        String s = new Integer(year).toString();
        if (digits == 2) {
            sb.append(s.substring(2,4));
        }
        else {
            sb.append(year);
        }

        int month = myCalendar.get(Calendar.MONTH);
        month++;
        s = new Integer(month).toString();
        if (month < 10) {
            sb.append("0");
            sb.append(s);
        }
        else {
            sb.append(s);
        }

        int day = myCalendar.get(Calendar.DAY_OF_MONTH);
        s = new Integer(day).toString();
        if (day < 10) {
            sb.append("0");
            sb.append(s);
        }
        else {
            sb.append(s);
        }

        return sb.toString();
    }

    public static String replaceAll(String source, String remove, String replace) {

        int foundPos = source.indexOf(remove);

        if (foundPos == -1) {
            return source;
        }

        int lastFoundPos;
        int lens = source.length();
        int lenr = remove.length();

        StringBuffer sb = new StringBuffer(source.substring(0,foundPos));
        sb.append(replace);

        foundPos = foundPos + lenr;
        lastFoundPos = foundPos;

        while ((foundPos = source.indexOf(remove, foundPos)) > -1) {
            sb.append(source.substring(lastFoundPos, foundPos));
            sb.append(replace);
            foundPos = foundPos + lenr;
            lastFoundPos = foundPos;
        }

        sb.append(source.substring(lastFoundPos, lens));

        return sb.toString();
    }

    public static  int getTimeStamp(Calendar c)  {
//	    if (! c.isSet(Calendar.HOUR_OF_DAY)) {
//	      throw new Exception("Year in calendar is not set");
//	    }
//	    if (! c.isSet(Calendar.MINUTE)) {
//	      throw new Exception("Year in calendar is not set");
//	    }
//	    if (! c.isSet(Calendar.SECOND)) {
//	      throw new Exception("Year in calendar is not set");
//	    }

        int hour = c.get(Calendar.HOUR_OF_DAY);
        int min = c.get(Calendar.MINUTE);
        int sec = c.get(Calendar.SECOND);
        int time = (hour * 10000) + (min * 100) + sec;

        return time;
    }
}
