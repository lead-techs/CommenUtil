/*
 * CAUtil.java  - Content Analysis Utility
 *
 */

package com.broaddata.common.util;

public class ClassWithCallBack 
{      
    private volatile Object data = null;
    private volatile Object total = null;
    
    public void sendBackData(Object data)
    {
        this.data = data;
    }

    public Object getTotal() {
        return total;
    }

    public void setTotal(Object total) {
        this.total = total;
    }
    
    public Object getData()
    {
        return data;
    }
    
    public void setData(Object data)
    {
        this.data = data;
    }
     
    public String getCategoryColumnValue(String categoryColumnCodeId,String categoryKey)
    {
        return "";
    }
    
    public String getCategoryColumnCodeId(int dataobjectType, String categoryColumnName)
    {
        return "";
    }
    
    public String showValueFromSelection(String codeKey,int dataType,String selectFrom)
    {
        return "";
    }
    
    public String getColumnSelectFrom(int dataobjectTypeId, String columnName)
    {
        return "";
    }
}
