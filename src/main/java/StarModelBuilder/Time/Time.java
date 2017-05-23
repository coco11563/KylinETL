package StarModelBuilder.Time;

import HbaseImporter.DatePart.dateUtil;
import HbaseImporter.HolidayPart.ChineseHoliday;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Sha0w on 2017/4/12.
 */
public class Time {

    private String date;
    private String year;
    private String month;
    private String day;
    private String is_holiday;
    private String time_id;

    public Time(Date date) throws ParseException {
        String dateStr = dateUtil.format(date);
        String[] dateSegment = dateStr.split(" ")[0].split("-");
        setYear(dateSegment[0]);
        setMonth(dateSegment[0] + dateSegment[1]);
        setDay(dateSegment[0] + dateSegment[1] + dateSegment[2]);
        setDate(dateUtil.formatFromNormal(date));
        setTime_id(dateUtil.formatFromNormal(date));
        setIs_holiday(String.valueOf(ChineseHoliday.getHoliday(date)));

    }


    private void generateId(Date date) {
        // TODO Auto-generated catch block
        String id = String.valueOf(date.getTime());
        setTime_id(id);
    }

    public String getTime_id() {
        return time_id;
    }

    public void setTime_id(String time_id) {
        this.time_id = time_id;
    }

    public String getIs_holiday() {
        return is_holiday;
    }

    public void setIs_holiday(String is_holiday) {
        this.is_holiday = is_holiday;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }


}
