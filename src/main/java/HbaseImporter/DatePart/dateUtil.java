package HbaseImporter.DatePart;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by root on 1/17/17.
 */
public class dateUtil {
    private static ThreadLocal<DateFormat> threadLocal = ThreadLocal.withInitial(() ->
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private static ThreadLocal<DateFormat> threadLocal_2 = ThreadLocal.withInitial(() ->
            new SimpleDateFormat("yyyy-MM-dd"));
    public static Date parse(String dateStr) throws ParseException {
        return threadLocal.get().parse(dateStr);
    }

    public static String format(Date date) {
        return threadLocal.get().format(date);
    }

    public static Date parseFromNormal(String dateStr) throws ParseException {
        return threadLocal_2.get().parse(dateStr);
    }
    public static String formatFromNormal(Date date) {
        return  threadLocal_2.get().format(date);
    }
}
