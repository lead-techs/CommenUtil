/*
 * ThriftUtil.java
 */

package com.broaddata.common.thrift;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class ThriftUtil 
{
    static final Logger log = Logger.getLogger("ThriftUtil");
   
    public static volatile ThreadLocal thriftConnectionKeys = new ThreadLocal();
    public static volatile Map<String,Map<String,Object>> thriftUserInfoMap = new HashMap<>();    
    
    public static int getClientUserId()
    {
        String thriftConnectionKey = (String)ThriftUtil.thriftConnectionKeys.get();
        
        Map<String,Object> userInfoMap = thriftUserInfoMap.get(thriftConnectionKey);
        
        if ( userInfoMap == null )
            return 0;
        else
            return (int) userInfoMap.get("userId");
    }
    
}
