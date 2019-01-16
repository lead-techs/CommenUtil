/*
 * Counter.java
 */

package com.broaddata.common.util;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
 
public class Counter 
{
    final Map<String, long[]> counter = new HashMap<>();
    Date firstAccessedTime = null;
    Date lastAccessedTime = null;
    
    public synchronized long increase(String key, long increaseNumber)  // key: processed_message_count, procssed_message_ms
    { 
        if ( firstAccessedTime == null )
            firstAccessedTime = new Date();
        
        lastAccessedTime = new Date();
        
        String currentSecond = Tool.convertDateToString(new Date(),"yyyyMMddHHmmss");
        String counterKey = String.format("%s:%s",currentSecond,key);
        
        long[] valueWrapper = counter.get(counterKey);

        if (valueWrapper == null) 
        {
            counter.put(counterKey, new long[] { increaseNumber });
            return increaseNumber;
        }
        else
        {
            valueWrapper[0] += increaseNumber;
            return valueWrapper[0];
        }
    }
    
    public long getValue(String key,Date startTime,Date endTime)
    {
        long value = 0;
        
        if ( !startTime.before(endTime) )
            return -1;
        
        if ( firstAccessedTime == null )
            return 0;
        
        Date date = startTime.before(firstAccessedTime)?firstAccessedTime:startTime;
        Date newEndTime = endTime.before(lastAccessedTime)?endTime:lastAccessedTime;
        
        while ( !date.after(newEndTime) )
        {
            String currentSecond = Tool.convertDateToString(date,"yyyyMMddHHmmss");
            String counterKey = String.format("%s:%s",currentSecond,key);            
            
            long[] valueWrapper = (long[])counter.get(counterKey);
                    
            if ( valueWrapper != null)
                value += valueWrapper[0];
                    
            date = Tool.dateAddDiff(date, 1, Calendar.SECOND);
        }
        
        return value;
    }
    
    public long getValue(String key,int timeType,int timeValue)
    {
        Date endTime = new Date();
        Date startTime = Tool.dateAddDiff(endTime, timeValue ,timeType);
        
        return getValue(key,startTime,endTime);
    }     
}
