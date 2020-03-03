package com.bi.analysis.util;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

/**
 * 日期时间工具类
 * @author Administrator
 *
 */
public class DateUtils {
	
	public static final SimpleDateFormat TIME_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat DATEKEY_FORMAT = 
			new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * 判断一个时间是否在另一个时间之前
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean before(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			if(dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 判断一个时间是否在另一个时间之后
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean after(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			if(dateTime1.after(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 计算时间差值（单位为秒）
	 * @param time1 时间1
	 * @param time2 时间2
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			long millisecond = datetime1.getTime() - datetime2.getTime();
			
			return Integer.valueOf(String.valueOf(millisecond / 1000));  
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取年月日和小时
	 * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
	 * @return 结果（yyyy-MM-dd_HH）
	 */
	public static String getDateHour(String datetime) {
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}  
	
	/**
	 * 获取当天日期（yyyy-MM-dd）
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		SimpleDateFormat timeFormat =
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return timeFormat.format(new Date());
	}

	//计算当前时间距离次日凌晨的时长(毫秒数)
	public static  Long getResetTime(){

//		Calendar cal =  Calendar.getInstance();
//		if(cal.get(Calendar.HOUR_OF_DAY)==13 && cal.get(Calendar.MINUTE)==0
//				&& cal.get(Calendar.SECOND)==0 && cal.get(Calendar.MILLISECOND)==0){
//			return 0L;
//		}
//		cal.set(cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DATE)+1,0,0,0);
//		return  cal.getTimeInMillis() - System.currentTimeMillis();

//		cal.add(Calendar.DAY_OF_YEAR,1);
//		cal.set(Calendar.HOUR_OF_DAY, 0);
//		cal.set(Calendar.MINUTE, 0);
//		cal.set(Calendar.SECOND, 0);
//		cal.set(Calendar.MILLISECOND, 0);

		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR,15);
		return cal.getTimeInMillis() - System.currentTimeMillis();
	}
	
	/**
	 * 获取昨天的日期（yyyy-MM-dd）
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -1);  
		
		Date date = cal.getTime();
		
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期（yyyy-MM-dd）
	 * @param date Date对象
	 * @return 格式化后的日期
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 * @param date Date对象
	 * @return 格式化后的时间
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}
	
	/**
	 * 解析时间字符串
	 * @param time 时间字符串 
	 * @return Date
	 */
	public static Date parseTime(String time) {
		try {
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化日期key
	 * @param date
	 * @return
	 */
	public static String formatDateKey(Date date) {
		return DATEKEY_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期key
	 * @param date
	 * @return
	 */
	public static Date parseDateKey(String datekey) {
		try {
			return DATEKEY_FORMAT.parse(datekey);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化时间，保留到分钟级别
	 * yyyyMMddHHmm
	 * @param date
	 * @return
	 */
	public static String formatTimeMinute(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");  
		return sdf.format(date);
	}

	/**
	 * 格式化时间，保留到分钟级别
	 * yyyyMMddHHmm
	 * @param 06/Dec/2017:13:32:32 +0800
	 */
	public static String formatTimeMinute(String dataStr){
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
			Date date = sdf.parse(dataStr);
			sdf = new SimpleDateFormat("yyyyMMddHHmm");
			return sdf.format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return  null;
	}

	/**
	 * 时间转换为时间戳
	 * @param s
	 * @return
	 * @throws ParseException
     */
	public static long dateToStamp(String s) throws ParseException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		Date date = simpleDateFormat.parse(s);
		long ts = date.getTime()/1000;
		return ts;
	}

	public static String tmpFormat(String s) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		Date date = simpleDateFormat.parse(s);
		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sd.format(date);
	}
}
