/*
 * ServiceNodeMBean.java 
 */

package com.broaddata.common.util;

import java.util.Date;

public interface EdfMBean 
{
    public int getServiceStatus();
    public long getPerformanceData(String key,Date startTime,Date endTime);
    public long getPerformanceData(String key,int timeType,int timeValue);
}
