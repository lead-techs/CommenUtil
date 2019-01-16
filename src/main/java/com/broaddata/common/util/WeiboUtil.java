
package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.Date;
import java.util.List;

public class WeiboUtil
{
    static final Logger log = Logger.getLogger("WeiboUtil");
       
    public static void login(String accountId, String password) throws Exception
    {
        
    }
    
    public static void followWeiboAccount(String accountId, String targetAccountId) throws Exception
    {
        
    }
    
    public static List<String> searchWeiboAccount(String keyword) throws Exception
    {
        
        return null;
    }
    
    public static String retrieveAccountPosts(String accountId, Date startTime, Date endTime, String keywordFilter,int maxPostNumber, boolean needComments, int maxCommentNumber) throws Exception // return post in json style
    {
        return "";
    }
    
    public String retrievePosts(Date startTime, Date endTime,String keywordFilter,int maxPostNumber, boolean needComments, int maxCommentNumber) throws Exception  // return post in json style
    {
        return "";
    }
}
