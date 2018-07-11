package com.ideal.flume.sink.hdfs;

import com.google.common.base.Preconditions;
import com.ideal.flume.Constants;
import com.ideal.flume.tools.DateTimeUtils;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by yukai on 2016/4/20.
 */
public class SimpleBucketPath {
  public static String simpleEscapeString(String in, Map<String, String> headers,
      boolean useLocalTimeStamp) {
    String ret = in;
    for (Map.Entry<String, String> entity : headers.entrySet()) {
      ret = StringUtils.replace(ret, entity.getKey(), entity.getValue());
    }

    
    boolean b1 = ret.indexOf("%Y") >= 0;
    boolean b2 = ret.indexOf("%m") >= 0;
    boolean b3 = ret.indexOf("%d") >= 0;
    boolean b4 = ret.indexOf("%H") >= 0;
    boolean b5 = ret.indexOf("%M") >= 0;
    
    if (!b1 && !b2 && !b3 && !b4&& !b5) {
      return ret;
    }
    
    long ts;
    if (useLocalTimeStamp) {
      ts = System.currentTimeMillis();
    } else {
      String timestampHeader = headers.get(Constants.TIMESTAMP);
      Preconditions.checkNotNull(timestampHeader, "Expected timestamp in "
          + "the Flume event headers, but it was null");
      ts = Long.parseLong(timestampHeader);
    }

    Calendar ca = Calendar.getInstance();
    ca.setTimeInMillis(ts);
    if (b1) {
      ret = StringUtils.replaceOnce(ret, "%Y", String.valueOf(ca.get(Calendar.YEAR)));
    }
    if (b2) {
      int tm = ca.get(Calendar.MONTH) + 1;
      ret = StringUtils.replaceOnce(ret, "%m", (tm < 10 ? "0" : "") + tm);
    }
    if (b3) {
      int tm = ca.get(Calendar.DAY_OF_MONTH);
      ret = StringUtils.replaceOnce(ret, "%d", (tm < 10 ? "0" : "") + tm);
    }
    if (b4) {
      int tm = ca.get(Calendar.HOUR_OF_DAY);
      ret = StringUtils.replaceOnce(ret, "%H", (tm < 10 ? "0" : "") + tm);
    }
    if (b5) {
        int tm = ca.get(Calendar.MINUTE);
        ret = StringUtils.replaceOnce(ret, "%M", (tm< 10 ? "0" : String.valueOf(tm).charAt(0))+"0");
      }

    return ret;
  }
  
	public String simpleEscapeString(String in, Map<String, String> headers, TimeZone timeZone, boolean needRounding, int unit,
			int roundDown, boolean useLocalTimeStamp,int criticalTime) {
		String ret = in;
		for (Map.Entry<String, String> entity : headers.entrySet()) {
			ret = StringUtils.replaceOnce(ret, entity.getKey(), entity.getValue());
		}

		long ts;
		if (useLocalTimeStamp) {
			ts = System.currentTimeMillis();
		} else {
			String timestampHeader = headers.get("timestamp");
			Preconditions.checkNotNull(timestampHeader, "Expected timestamp in " + "the Flume event headers, but it was null");
			ts = Long.valueOf(timestampHeader);
		}

		Calendar ca = Calendar.getInstance();
		ca.setTimeInMillis(ts);
		int hour = ca.get(Calendar.HOUR_OF_DAY);
		if (ret.indexOf("%Y") >= 0) {
			ret = StringUtils.replaceOnce(ret, "%Y", String.valueOf(ca.get(Calendar.YEAR)));
		}
		if (ret.indexOf("%m") >= 0) {
			int t = ca.get(Calendar.MONTH) + 1;
			ret = StringUtils.replaceOnce(ret, "%m", (t < 10 ? "0" : "") + t);
		}
		if (ret.indexOf("%d") >= 0) {
			int t = ca.get(Calendar.DAY_OF_MONTH);
			//处理临界时间业务
			if(useLocalTimeStamp && (hour <  criticalTime)){
				t = t-1;
			}
			ret = StringUtils.replaceOnce(ret, "%d", (t < 10 ? "0" : "") + t);
		}
		if (ret.indexOf("%H") >= 0) {
			int t = ca.get(Calendar.HOUR_OF_DAY);
			String hstr = (t < 10 ? "0" : "") + t;
			//处理临界时间业务
			if(useLocalTimeStamp && (hour <  criticalTime)){
				int year = ca.get(Calendar.YEAR);
				int month = ca.get(Calendar.MONTH)+1;
				int day = ca.get(Calendar.DAY_OF_MONTH);
				hstr = year+(month<10?"0"+month:month+"")+(day<10?"0"+day:""+day)+hstr;
			}
			ret = StringUtils.replaceOnce(ret, "%H", hstr);
		}

		return ret;
	}



	public static void main(String[] args) throws ParseException {
		
		System.out.println(simpleEscapeString("%Y%m%d/%H/%M/",new HashMap<String,String>(),true));
		
		Calendar ca = Calendar.getInstance();
		ca.set(Calendar.MINUTE, 0);
//		System.out.println(ca.get(Calendar.YEAR));
//		System.out.println(ca.get(Calendar.MONTH) + 1);
//		System.out.println(ca.get(Calendar.DAY_OF_MONTH));
//		System.out.println(ca.get(Calendar.HOUR_OF_DAY));
		System.out.println(ca.get(Calendar.MINUTE));
		
		String ret = "/pubic/test/%Y-%m-%d/%H/%M";
		 int tm = ca.get(Calendar.MINUTE);
		 System.out.println("time : "+tm);
		 System.out.println("value:"+String.valueOf(tm).charAt(0));
		 System.out.println(tm < 10 );
		 ret = StringUtils.replaceOnce(ret, "%M", (tm< 10 ? "0" : String.valueOf(tm).charAt(0))+"0");
	      System.out.println(ret);
		/*String path = "/pubic/test/%Y-%m-%d/%H/%M";
		String str = path.substring(path.indexOf("%Y"),path.indexOf("%d")+2);
		System.out.println(str);
		System.out.println(DateTimeUtils.getHour24());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(DateTimeUtils.parseDate("20170101","yyyyMMdd"));
		calendar.add(Calendar.DATE,Integer.valueOf("-2"));
		System.out.println(DateTimeUtils.customDateTime(calendar,"yyyyMMdd"));
		calendar.add(Calendar.DATE,3);
		System.out.println(DateTimeUtils.customDateTime(calendar,"yyyyMMdd"));*/

	}

}
