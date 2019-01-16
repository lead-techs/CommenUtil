/*
 * ThreadLocalWrapper.java
 */

package com.broaddata.common.util;

import java.util.HashMap;
import java.util.Map;
 
public class ThreadLocalWrapper 
{
    public static final ThreadLocal userThreadLocal = new ThreadLocal();

    public static void setContext(Map<String,Object> context) {
        userThreadLocal.set(context);
    }

    public static void remove() {
        userThreadLocal.remove();
    }

    public static Map<String,Object> getContext() {
        return (Map<String,Object>)userThreadLocal.get();
    }
    
    public static Object getContext(String key) {
        Map<String,Object> map = (Map<String,Object>)userThreadLocal.get();
        
        if ( map == null )
            return null;
        else
            return map.get(key);
    }
    
    public static int getUserId()
    {
        Map<String,Object> map = (Map<String,Object>)userThreadLocal.get();
       
        if ( map == null )
            return 0;
        else
            return (Integer)map.get("userId");
    }
    
    public static void setUserId(int userId) {
        Map<String,Object> context = new HashMap<>();
        context.put("userId", userId);
        userThreadLocal.set(context);
    }
}