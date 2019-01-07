import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

import static java.time.format.DateTimeFormatter.ofPattern;

/**
 * Created by Madhouse on 2018/4/24.
 */

public class Main {
    private static void function() {
        LocalDateTime ldt = LocalDateTime.now();
        Timestamp t = Timestamp.valueOf(ldt);
        System.out.println("time (" + ZoneId.systemDefault() + ") = " + t);
        ZoneId zoneId = ZoneId.of("Europe/Brussels");
        Timestamp t2 = Timestamp.from(ldt.atZone(zoneId).toInstant());
        System.out.println("time (" + zoneId + ") = " + t2);
    }

    private static Long nowTimestamp() {
        return System.currentTimeMillis();
    }

    private static String timeStamp2Date(Long ts, String zone) {
        long t = ts;
        LocalDateTime triggerTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(t),
                ZoneId.of(zone));
        return triggerTime.toString();
    }

    private static Long date2Timestamp(String date, String zone) {
        String PATTERN = "yyyy-MM-dd HH:mm:ss";
        LocalDateTime l = LocalDateTime.parse(date, ofPattern(PATTERN));
        Long ts = Timestamp.from(l.atZone(ZoneId.of(zone)).toInstant()).getTime();
        return ts;
    }

    private static String double2String(double d) {
        String minute = ":00";
        String hour = String.valueOf(d);
        if (hour.contains(".")) {
            String[] str = hour.split("\\.");
            hour = str[0];
            if(str[1].charAt(0) != '0'){
                minute = ":30";
            }
        }
        String res = "";
        if (d > 0) {
            res = "+" + hour + minute;
        } else {
            res = hour + minute;
        }
        return res;
    }

    public static void main(String[] args) {
        //function();
        Long now = nowTimestamp();
        String z1 = "UTC";
        String z2 = "GMT+8";
        String z3 = "GMT+9";
        String z4 = "+09:30";
        System.out.println("now = " + 1527094800 + ", zoneid = " + z1 + ", date = " + timeStamp2Date(1527094801000L, z1));
        System.out.println("now = " + 1527094800 + ", zoneid = " + z2 + ", date = " + timeStamp2Date(1527094800000L, z2));
        System.out.println("now = " + 1527094800 + ", zoneid = " + z3 + ", date = " + timeStamp2Date(1527094800000L, z3));
        System.out.println("now = " + 1527094800 + ", zoneid = " + z4 + ", date = " + timeStamp2Date(1527094800000L, z4));

        String d1 = "2018-06-24 00:00:00";
        String d2 = "2018-06-24 00:00:00";
        String d3 = "2018-06-23 00:00:00";
        String d4 = "2018-06-24 00:00:00";
        System.out.println("date = " + d1 + ", zoneid = " + z1 + ", ts = " + date2Timestamp(d1, z1));
        System.out.println("date = " + d2 + ", zoneid = " + z2 + ", ts = " + date2Timestamp(d2, z2));
        System.out.println("date = " + d3 + ", zoneid = " + z3 + ", ts = " + date2Timestamp(d3, z3));
        System.out.println("date = " + d4 + ", zoneid = " + z3 + ", ts = " + date2Timestamp(d4, z3));

        /*double[] ds = {1,1.0,5.5,-10,-10.0,8,-8.5};
        for (double d : ds) {
            System.out.println(double2String(d));
        }*/



    }
}
