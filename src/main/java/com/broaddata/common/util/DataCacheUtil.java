/*
 * DataCacheUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.vo.DataobjectBody;

public class DataCacheUtil 
{      
    static final Logger log=Logger.getLogger("DataCacheUtil");      

    public static String dataCacheUrl = "127.0.0.1";
    public static JedisPool jedisPool = null;
    
    public static synchronized void initRedis(String url)
    {
        dataCacheUrl = url;
        JedisPoolConfig config = new JedisPoolConfig();
        
        config.setMaxTotal(20);
        config.setMaxIdle(10);
        config.setMinIdle(0);
        //config.setBlockWhenExhausted(true);
        //config.setMaxWaitMillis(100*1000);

        jedisPool = new JedisPool(config, dataCacheUrl);
    }
            
    public static long getDataCacheSize()
    {
        Jedis jedis = null;

        try
        {                              
            jedis = DataCacheUtil.jedisPool.getResource();  
            return jedis.dbSize();
        }
        catch(Exception e)
        {
            log.error("getDBSize() failed! e="+e);
            return -1;
        }
        finally 
        {  
            try
            {
              if ( jedis != null )
                  jedis.close();
            }
            catch(Exception e) {
            }
        }  
    }
    
    public static void saveObjectToCache(Jedis jedis,String key, Object object) throws Exception
    {
        jedis.set(key.getBytes(),Tool.serializeObject(object));
    }
    
    public static void setExpiration(Jedis jedis,String key,int seconds) throws Exception
    {
        jedis.expire(key.getBytes(), seconds);
    }
    
    public static Object getObjectFromCache(Jedis jedis,String key) throws Exception
    {
        byte[] buf = jedis.get(key.getBytes());

        if ( buf == null )
            return null;
        else
            return Tool.deserializeObject(buf);
    }
        
    public static void saveDataobjectBodyToCache(DataobjectBody dataobjectBody) throws Exception
    {
        int retry = CommonKeys.DATA_CACHE_RETRY_NUMBER;

        while(true)
        {
            retry --;

            Jedis jedis = null;

            try
            {                
                jedis = jedisPool.getResource();  
                saveObjectToCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT+dataobjectBody.getDataobjectId(),dataobjectBody);
                break;
            }
            catch(Exception e)
            {
                log.error("save dataobject status failed! e="+e+"dataobject="+dataobjectBody.getDataobjectId());
                Tool.SleepAWhile(CommonKeys.DATA_CACHE_RETRY_WAITING_TIME_IN_SECONDS, 0);

                if ( retry==0 )
                    throw e;
            }
            finally 
            {  
                try
                {
                  if ( jedis != null )
                      jedis.close();
                }
                catch(Exception e) {
                }
            }  
        }
    }
    
    public static Dataobject getDataobjectFromCache(String dataobjectId) throws Exception
    {
        Dataobject dataobject = null;
        DataobjectBody dataobjectBody = null;
        Jedis jedis = null;
        
        int retry = CommonKeys.DATA_CACHE_RETRY_NUMBER;

        while(retry>0)
        {
            retry --;

            try
            {                
                jedis = jedisPool.getResource();  
                
                dataobjectBody = (DataobjectBody)getObjectFromCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT+dataobjectId);
                                
                if ( dataobjectBody == null )
                {
                    String errorInfo = " key "+(CommonKeys.DATA_CACHE_KEY_DATAOBJECT+dataobjectId)+" not found!!!";
                    log.error(errorInfo);
                    
                    throw new Exception(errorInfo);
                }
                
                dataobject = dataobjectBody.getDataobject();
                
                if ( !dataobject.getId().equals(dataobjectId) )
                {
                    String errorInfo = "dataobject id not match! dataobjectId="+dataobjectId+",dataobject.getId()="+dataobject.getId();
                    log.error(errorInfo);
                    throw new Exception(errorInfo);
                }
                
                break;
            }
            catch(Exception e)
            {
                log.error("get dataobject failed! e="+e+"dataobjectId="+dataobjectId);
                Tool.SleepAWhile(CommonKeys.DATA_CACHE_RETRY_WAITING_TIME_IN_SECONDS, 0);

                if ( retry==0 )
                    throw e;
            }
            finally 
            {  
                try
                {
                  if ( jedis != null )
                      jedis.close();
                }
                catch(Exception e) {
                }
            }  
        }
        
        return dataobject;
    }
    
    public static long removeDataobjectAndBodyFromCache(String dataobjectId,String dataobjectChecksum) throws Exception
    {
        Jedis jedis = null;
        int retry = CommonKeys.DATA_CACHE_RETRY_NUMBER;
        long deleted = 0;

        while(retry>0)
        {
            retry --;

            try
            {                    
                jedis = jedisPool.getResource();  
                
                DataobjectBody dataobjectBody = (DataobjectBody)getObjectFromCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT+dataobjectId);
                
                if ( dataobjectBody == null )
                {
                    log.warn("remove dataobject body not found!  dataobjectId="+dataobjectId);
                    continue;
                }
                 
                Dataobject dataobject = dataobjectBody.getDataobject();
                
                //Dataobject dataobject = (Dataobject)getObjectFromCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT_HEAD+dataobjectId);
                
                if ( dataobject == null )
                {
                    log.warn("remove dataobject not found!  dataobjectId="+dataobjectId);
                    continue;  
                }

                if ( !dataobject.getChecksum().trim().equals(dataobjectChecksum) )  // there is new data updated, no need to delete
                {
                    log.warn(" dataobject checksum changed!");
                    break;
                }

                deleted = jedis.del((CommonKeys.DATA_CACHE_KEY_DATAOBJECT+dataobjectId).getBytes());
                
                if ( deleted < 1 )
                    continue;

                break;
            }
            catch(Exception e)
            {
                log.error("removeDataobjectAndBodyFromCache failed! e="+e+"dataobjectId="+dataobjectId);
                Tool.SleepAWhile(CommonKeys.DATA_CACHE_RETRY_WAITING_TIME_IN_SECONDS, 0);

                if ( retry==0 )
                    throw e;
            }
            finally 
            {  
                try
                {
                  if ( jedis != null )
                      jedis.close();
                }
                catch(Exception e) {
                }
            }  
        }
        
        return deleted;
    }   
    
    public static long removeDataobjectAndBodyFromCacheByKey(String dataCacheKey) throws Exception
    {
        Jedis jedis = null;
        int retry = CommonKeys.DATA_CACHE_RETRY_NUMBER;
        long deleted = 0;

        while(retry>0)
        {
            retry --;

            try
            {                    
                jedis = jedisPool.getResource();  
           
                DataobjectBody dataobjectBody = (DataobjectBody)getObjectFromCache(jedis, dataCacheKey);
                
                if ( dataobjectBody == null )
                {
                    log.warn("remove dataobject body not found!  dataCacheKey="+dataCacheKey);
                    continue;
                }
                 
                Dataobject dataobject = dataobjectBody.getDataobject();
        
                if ( dataobject == null )
                {
                    log.warn("remove dataobject not found!  dataCacheKey="+dataCacheKey);
                    continue;  
                }

                deleted = jedis.del(dataCacheKey.getBytes());
                
                if ( deleted < 1 )
                    continue;

                break;
            }
            catch(Exception e)
            {
                log.error("removeDataobjectAndBodyFromCache failed! e="+e+"dataCacheKey="+dataCacheKey);
                Tool.SleepAWhile(CommonKeys.DATA_CACHE_RETRY_WAITING_TIME_IN_SECONDS, 0);

                if ( retry==0 )
                    throw e;
            }
            finally 
            {  
                try
                {
                  if ( jedis != null )
                      jedis.close();
                }
                catch(Exception e) {
                }
            }  
        }
        
        return deleted;
    }   
    
    public static List<DataobjectBody> getDataobjectBodyListFromCache(String keyStr,Date startTime, Date endTime, int indexStatus, int dataobjectTypeId, int maxItemNumber) throws Exception
    {
        DataobjectBody dataobjectBody;
        Dataobject dataobject;
        List<DataobjectBody> dataobjectBodyList = new ArrayList<>();
                           
        int retry = CommonKeys.DATA_CACHE_RETRY_NUMBER;

        Jedis jedis = null;

        while(retry>0)
        {
            retry --;

            try 
            {                                     
                jedis = DataCacheUtil.jedisPool.getResource();  

                dataobjectBodyList = new ArrayList<>();
                String pattern = keyStr+CommonKeys.DATA_CACHE_KEY_DATAOBJECT+"*";
                Set<byte[]> set = jedis.keys(pattern.getBytes());
               
                int k = 0;
  
                for (byte[] key : set)
                { 
                    byte[] val = jedis.get(key);
                    dataobjectBody = (DataobjectBody)Tool.deserializeObject(val);
                    dataobject = dataobjectBody.getDataobject();
                    
                    if ( indexStatus > 0 && dataobject.getIndexStatus() != indexStatus )
                        continue;
                    
                    if ( dataobjectTypeId > 0 && dataobject.getDataobjectType() != dataobjectTypeId )
                        continue;
                    
                    if ( dataobject.getIndexStatusTime().before(startTime) || dataobject.getIndexStatusTime().after(endTime) )
                        continue;
                    
                    dataobjectBodyList.add(dataobjectBody);
                    
                    k++;
                  
                    if ( k >= maxItemNumber )
                        break;
                }               

                break;
            }
            catch(Exception e)
            {
                log.error("getDataobjectBodyListFromCache() failed! e="+e);
                Tool.SleepAWhile(CommonKeys.DATA_CACHE_RETRY_WAITING_TIME_IN_SECONDS, 0);

                if ( retry==0 )
                    throw e;
            }
            finally {  
                try
                {
                  if ( jedis != null )
                      jedis.close();
                }
                catch(Exception e) {
                }
            }  
        }
            
        return dataobjectBodyList;
    }
}
