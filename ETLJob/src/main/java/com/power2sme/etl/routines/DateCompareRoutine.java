package com.power2sme.etl.routines;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.power2sme.etl.constants.ETLConstants;

public class DateCompareRoutine {

	public static final String dateFormat = ETLConstants.DATE_FORMAT;
	
	/*
	 * public static int compareDate(Object date1, Object date2 ) { if(date1 == null
	 * && date2 == null) return 0; if(date1 == null || !(date1 instanceof Date))
	 * return -1; if(date2 == null || ) return 1;
	 * 
	 * long time1 = date1.getTime(), time2 = date2.getTime(); return (time1 < time2
	 * ? -1 : (time1 == time2 ? 0 : 1)); }
	 */
	
	public static int compareDate(Date date1, Date date2 )
	{
		if(date1 == null && date2 == null)
			return 0;
		if(date1 == null)
			return -1;
		if(date2 == null)
			return 1;
		long time1 = date1.getTime(), time2 = date2.getTime();
		return (time1 < time2 ? -1 : (time1 == time2 ? 0 : 1));
	}
	
	public static int compareDate(Date date1, String date2 )
	{
	
		SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
		Date secondDate = null;
		try {
			secondDate = formatter.parse(date2);
		} catch (ParseException e) {
			
			secondDate = null;
		}
		
		return compareDate(date1, secondDate);
	}
	
	public static int compareDate(String date1, Date date2 )
	{
		SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
		Date firstDate = null;
		try {
			firstDate = formatter.parse(date1);
		} catch (ParseException e) {
			
			firstDate = null;
		}
		
		return compareDate(firstDate, date2);
		
	}
	
	public static int compareDate(String date1, String date2 )
	{
	
		SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
		Date secondDate = null;
		Date firstDate = null;
		try {
			secondDate = formatter.parse(date2);
		} catch (ParseException e) {
			
			secondDate = null;
		}
		try {
			firstDate = formatter.parse(date1);
		} catch (ParseException e) {
			
			firstDate = null;
		}
		
		return compareDate(firstDate, secondDate);
	}
}
