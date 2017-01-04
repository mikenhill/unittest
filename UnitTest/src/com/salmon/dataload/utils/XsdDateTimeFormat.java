package com.salmon.dataload.utils;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Methods for parsing XML Schema date/time data types.
 * 
 * @author mwebster
 * @see http://www.w3.org/TR/2004/PER-xmlschema-2-20040318/datatypes.html#dateTime
 * @see http://www.w3.org/TR/2004/PER-xmlschema-2-20040318/datatypes.html#time
 * @see http://www.w3.org/TR/2004/PER-xmlschema-2-20040318/datatypes.html#date
 */
public class XsdDateTimeFormat {
	public static final String CLASS_NAME = XsdDateTimeFormat.class.getName();
	public static final String TIME_ZONE_REGEX = "([-\\+]\\d{2}:\\d{2}|Z)?";
	public static final String DATE_REGEX = "([-]?\\d{4,})-(\\d{2})-(\\d{2})";
	public static final String TIME_REGEX = "(\\d{2}):(\\d{2}):(\\d{2})(\\.\\d+)?";
	public static final String DATE_FULL_REGEX = DATE_REGEX + TIME_ZONE_REGEX;
	public static final String TIME_FULL_REGEX = TIME_REGEX + TIME_ZONE_REGEX;
	public static final String DATE_TIME_FULL_REGEX = DATE_REGEX + 'T' + TIME_REGEX + TIME_ZONE_REGEX;
	public static final int MILLIS_PER_SEC = 1000;

	/**
	 * Converts from XSD date type to JDBC date type.
	 * 
	 * @param xsdDate
	 * @return date
	 * @see http://www.w3.org/TR/2004/PER-xmlschema-2-20040318/datatypes.html#date
	 */
	public static java.sql.Date parseDate(String xsdDate) {
		final String METHOD_NAME = "parseDate";
		java.sql.Date date = null;
		if (xsdDate != null && xsdDate.length() > 0) {
			Pattern pattern = Pattern.compile(DATE_FULL_REGEX);
			Matcher matcher = pattern.matcher(xsdDate);
			if (matcher.matches()) {
				Calendar cal = new GregorianCalendar();
				cal.set(Calendar.YEAR, Integer.parseInt(matcher.group(1)));
				cal.set(Calendar.MONTH, Integer.parseInt(matcher.group(2)) - 1); // months start at zero.
				cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(matcher.group(3)));
				cal.setTimeZone(getTimeZone(matcher.group(4)));
				date = new java.sql.Date(cal.getTimeInMillis());
			} else {
				throw new IllegalArgumentException(CLASS_NAME + '.' + METHOD_NAME + ": Argument \"" + xsdDate + "\" does not match XSD date format.");
			}
		}
		return date;
	}

	/**
	 * Converts from XSD time type to JDBC time type.
	 * 
	 * @param xsdTime
	 * @return time
	 * @see http://www.w3.org/TR/2004/PER-xmlschema-2-20040318/datatypes.html#time
	 */
	public static Time parseTime(String xsdTime) {
		final String METHOD_NAME = "parseTime";
		Time time = null;
		if (xsdTime != null && xsdTime.length() > 0) {
			Pattern pattern = Pattern.compile(TIME_FULL_REGEX);
			Matcher matcher = pattern.matcher(xsdTime);
			if (matcher.matches()) {
				Calendar cal = new GregorianCalendar();
				cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(matcher.group(1)));
				cal.set(Calendar.MINUTE, Integer.parseInt(matcher.group(2)));
				cal.set(Calendar.SECOND, Integer.parseInt(matcher.group(3)));
				cal.set(Calendar.MILLISECOND, getMilliseconds(matcher.group(4)));
				cal.setTimeZone(getTimeZone(matcher.group(5)));
				time = new Time(cal.getTimeInMillis());
			} else {
				throw new IllegalArgumentException(CLASS_NAME + '.' + METHOD_NAME + ": Argument \"" + xsdTime + "\" does not match XSD time format.");
			}
		}
		return time;
	}

	/**
	 * Converts from XSD dateTime type to JDBC timestamp type.
	 * 
	 * @param xsdDate
	 * @return timestamp
	 * @see http://www.w3.org/TR/2004/PER-xmlschema-2-20040318/datatypes.html#dateTime
	 */
	public static Timestamp parseDateTime(String xsdDateTime) {
		final String METHOD_NAME = "parseDateTime";
		Timestamp timestamp = null;
		if (xsdDateTime != null && xsdDateTime.length() > 0) {
			Pattern pattern = Pattern.compile(DATE_TIME_FULL_REGEX);
			Matcher matcher = pattern.matcher(xsdDateTime);
			if (matcher.matches()) {
				Calendar cal = new GregorianCalendar();
				cal.set(Calendar.YEAR, Integer.parseInt(matcher.group(1)));
				cal.set(Calendar.MONTH, Integer.parseInt(matcher.group(2)) - 1); // months start at zero.
				cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(matcher.group(3)));
				cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(matcher.group(4)));
				cal.set(Calendar.MINUTE, Integer.parseInt(matcher.group(5)));
				cal.set(Calendar.SECOND, Integer.parseInt(matcher.group(6)));
				cal.set(Calendar.MILLISECOND, getMilliseconds(matcher.group(7)));
				cal.setTimeZone(getTimeZone(matcher.group(8)));
				timestamp = new Timestamp(cal.getTimeInMillis());
			} else {
				throw new IllegalArgumentException(CLASS_NAME + '.' + METHOD_NAME + ": Argument \"" + xsdDateTime + "\" does not match XSD dateTime format.");
			}
		}
		return timestamp;
	}

	private static TimeZone getTimeZone(String xsdTimeZone) {
		TimeZone timeZone = null;
		if (xsdTimeZone == null) {
			timeZone = TimeZone.getDefault();
		} else {
			if ("Z".equals(xsdTimeZone)) {
				timeZone = TimeZone.getTimeZone("GMT");
			} else {
				timeZone = TimeZone.getTimeZone("GMT" + xsdTimeZone);
			}
		}
		return timeZone;
	}

	private static int getMilliseconds(String fractionOfSecond) {
		int millis = 0;
		if (fractionOfSecond != null) {
			millis = (int) (Double.parseDouble(fractionOfSecond) * MILLIS_PER_SEC);
		}
		return millis;
	}

}
