package com.adatafun.computing.platform.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.lang3.time.DateUtils.parseDate;

/**
 * DateUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/19.
 */
public class DateUtil {

    private static List<String> getWorkDay() { // 周末
        List<String> workDay = new ArrayList<>();
        workDay.add("2018-01-06");
        return workDay;
    }

    private static List<String> getHolidays() { //节假日
        List<String> holidays = new ArrayList<>();
        holidays.add("2017-01-01");//元旦
        holidays.add("2017-01-02");
        holidays.add("2017-01-03");

        holidays.add("2017-02-18");//春节
        holidays.add("2017-02-19");
        holidays.add("2017-02-20");
        holidays.add("2017-02-21");
        holidays.add("2017-02-22");
        holidays.add("2017-02-23");
        holidays.add("2017-02-24");

        holidays.add("2017-04-04");//清明节
        holidays.add("2017-04-05");
        holidays.add("2017-04-06");

        holidays.add("2017-04-29");//劳动节
        holidays.add("2017-04-30");
        holidays.add("2017-05-01");

        holidays.add("2017-06-20");//端午节
        holidays.add("2017-06-21");
        holidays.add("2017-06-22");

        holidays.add("2017-09-27");//中秋节

        holidays.add("2017-10-01");//国庆节
        holidays.add("2017-10-02");
        holidays.add("2017-10-03");
        holidays.add("2017-10-04");
        holidays.add("2017-10-05");
        holidays.add("2017-10-06");
        holidays.add("2017-10-07");
        return holidays;
    }

    public Map<String, String> getDateInterval(int field, int amount) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//HH:24小时制，hh:12小时制
        String minDate = simpleDateFormat.format(new Date());

        System.out.println(minDate);
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(simpleDateFormat.parse(minDate));
        String stopTime = simpleDateFormat.format(calendar.getTime());
        calendar.add(field, amount);//正数往后推,负数往前移动
        String startTime = simpleDateFormat.format(calendar.getTime());

        Map<String, String> map = new HashMap<>();
        map.put("startTime", startTime);
        map.put("stopTime", stopTime);

        return map;
    }

    /**
     *
     * @param date1 <String>
     * @param date2 <String>
     * @return Integer
     */
    public Integer getDaySpace(String date1, String date2) {

        Integer result;

        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();

        if (date1.equals("")) {
            return -1;
        }
        if (date2.equals("")) {
            date2 = getDate(new Date());
        }

        c1.setTime(getDate(date1));
        c2.setTime(getDate(date2));

        result = c2.get(Calendar.DAY_OF_MONTH) - c1.get(Calendar.DAY_OF_MONTH);

        return result;

    }

    /**
     *
     * @param date1 <String>
     * @param date2 <String>
     * @return Integer
     */
    public Integer getMinuteSpace(String date1, String date2) {

        Integer result;

        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();

        if (date2.equals("")) {
            date2 = getDate(new Date());
        }

        c1.setTime(getDate(date1));
        c2.setTime(getDate(date2));

        result = c2.get(Calendar.MINUTE) - c1.get(Calendar.MINUTE);

        return result;

    }

    /**
     * 判断一个日期是否是节假日 法定节假日只判断月份和天，不判断年
     *
     * @param date 日期
     * @return 布尔值
     */
    public boolean isHoliday(Date date) {
        boolean holiday = false;
        Calendar fcal = Calendar.getInstance();
        Calendar dcal = Calendar.getInstance();
        dcal.setTime(date);
        List<String> list = getHolidays();
        for (String dt : list) {
            fcal.setTime(this.getDate(dt));
            fcal.set(Calendar.YEAR, dcal.get(Calendar.YEAR));

            // 法定节假日判断
            if (fcal.get(Calendar.MONTH) == dcal.get(Calendar.MONTH)
                    && fcal.get(Calendar.DATE) == dcal.get(Calendar.DATE)) {
                holiday = true;
            } else {//法定节假日前后5天判断
                fcal.add(Calendar.DAY_OF_MONTH, -5);
                Date f_before = fcal.getTime();
                fcal.add(Calendar.DAY_OF_MONTH, 10);
                Date f_after = fcal.getTime();
                if (date.compareTo(f_before) == 1 && date.compareTo(f_after) == -1) {
                    holiday = true;
                }
            }
        }
        return holiday;
    }

    /**
     * 航班起飞时间段判断：22:00-次日5:59内，6:00-9:59内，10:00-13:59内，4:00-17:59内，18:00-21:59内
     *                     0，1，2，3，4
     * @param date 日期
     * @return 布尔值
     */
    public Integer matchTimeSlot(Date date) {

        Integer slot;
        Calendar dcal = Calendar.getInstance();
        dcal.setTime(date);

        Integer hour = dcal.get(Calendar.HOUR_OF_DAY);
        if (hour >= 6 && hour < 10) {
            slot = 1;
        } else if (hour >= 10 && hour < 14) {
            slot = 2;
        } else if (hour >= 14 && hour < 18) {
            slot = 3;
        } else if (hour >= 18 && hour < 22) {
            slot = 4;
        } else {
            slot = 0;
        }
        return slot;
    }

    /**
     * 周五周六周日判断：根据公司需求修改
     *
     * @param date 日期
     * @return 布尔值
     */
    public boolean isWeekend(Date date) {
        boolean weekend = false;
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
                || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY
                || cal.get(Calendar.DAY_OF_WEEK) == Calendar.FRIDAY) {
            weekend = true;
        }
        return weekend;
    }

    /**
     * 是否是工作日 法定节假日和周末为非工作日
     *
     * @param date 日期
     * @return 布尔值
     */
    public boolean isWorkDay(Date date) {
        boolean workday = true;
        if (this.isHoliday(date) || this.isWeekend(date)) {
            workday = false;
        }

  /* 特殊工作日判断 */
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date);
        Calendar cal2 = Calendar.getInstance();
        for (String dt : getWorkDay()) {
            cal2.setTime(getDate(dt));
            if (cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR)
                    && cal1.get(Calendar.MONTH) == cal2.get(Calendar.MONTH)
                    && cal1.get(Calendar.DATE) == cal2.get(Calendar.DATE)) { // 年月日相等为特殊工作日
                workday = true;
            }
        }
        return workday;
    }

    public Date getDate(String str) {
        Date dt = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            dt = df.parse(str);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return dt;

    }

    public Date getDateTime(String str) {
        Date dt = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            dt = df.parse(str);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return dt;

    }

    public Date setTimeZone(Date date) throws ParseException {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, -8);//把日期往前减八个小时,矫正es时间差
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
//        String dateString = simpleDateFormat.format(date);
//        Date result = simpleDateFormat.parse(dateString);
        return calendar.getTime();
    }

    public int getDateSpace(String date1, String date2) throws ParseException {

        Calendar calst = Calendar.getInstance();
        Calendar caled = Calendar.getInstance();

        calst.setTime(parseDate("yyyyMMdd",date1));
        caled.setTime(parseDate("yyyyMMdd",date2));

        //设置时间为0时
        calst.set(Calendar.HOUR_OF_DAY, 0);
        calst.set(Calendar.MINUTE, 0);
        calst.set(Calendar.SECOND, 0);
        caled.set(Calendar.HOUR_OF_DAY, 0);
        caled.set(Calendar.MINUTE, 0);
        caled.set(Calendar.SECOND, 0);
        //得到两个日期相差的天数
        int days = ((int)(caled.getTime().getTime()/1000)-(int)(calst.getTime().getTime()/1000))/3600/24;

        return days;
    }

    public static int daysBetween(Date early, Date late) {

        java.util.Calendar calst = java.util.Calendar.getInstance();
        java.util.Calendar caled = java.util.Calendar.getInstance();
        calst.setTime(early);
        caled.setTime(late);
        //设置时间为0时
        calst.set(java.util.Calendar.HOUR_OF_DAY, 0);
        calst.set(java.util.Calendar.MINUTE, 0);
        calst.set(java.util.Calendar.SECOND, 0);
        caled.set(java.util.Calendar.HOUR_OF_DAY, 0);
        caled.set(java.util.Calendar.MINUTE, 0);
        caled.set(java.util.Calendar.SECOND, 0);
        //得到两个日期相差的天数
        int days = ((int) (caled.getTime().getTime() / 1000) - (int) (calst
                .getTime().getTime() / 1000)) / 3600 / 24;

        return days;
    }

    public String getDate(Date date) {
        String dt;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        dt = df.format(date);

        return dt;

    }

    /**
     * @param args 入参
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Date date=new Date();//取时间

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = formatter.format(date);

        System.out.println(date);

        DateUtil f = new DateUtil();
        Date dt = f.getDate(dateString);
        Date ddd = f.setTimeZone(date);
        System.out.println("ddd: "+ddd);
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
//        System.out.println(simpleDateFormat.parse(ddd));
//        System.out.println(f.isWorkDay(dt));
//        System.out.println(f.isHoliday(dt));
//        System.out.println(f.matchTimeSlot(dt));
//        System.out.println(f.getDaySpace(dateString, dateString));
//        System.out.println("非节假日出行".compareTo("节假日出行"));
    }

}
