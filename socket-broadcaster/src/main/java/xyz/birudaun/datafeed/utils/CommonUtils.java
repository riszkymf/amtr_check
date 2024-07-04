package xyz.birudaun.datafeed.utils;

public class CommonUtils {
  public static final String getDateWithSeparator(String date) {
    return getDateWithSeparator(date, "-");
  }
  
  public static final String getDateWithSeparator(String date, String separator) {
    return String.valueOf(date.substring(0, 4)) + separator + date.substring(4, 6) + separator + date.substring(6);
  }
  
  public static final String getTimeWithSeparator(String time, String separator) {
    return String.valueOf(time.substring(0, 2)) + separator + time.substring(2);
  }
}
