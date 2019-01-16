/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.vo.DataobjectBody;
import com.broaddata.common.model.vo.DataobjectVO;
import javax.persistence.EntityManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 *
 * @author edf
 */
public class DataCacheUtilTest {
    
    public DataCacheUtilTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
 
    @Ignore
    @Test
    public void testAAA() throws Exception 
    {
        
        String dataobjectId = "577C026DFD261B70C3487520F8BFC844001AB809";
                
        System.out.println("saveObjectToCache");
        
        Util.commonServiceIPs = "127.0.0.1";
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
            
        String dataCacheUrl = Util.getSystemConfigValue(platformEm,0,"data_cache","data_cache_url");
        DataCacheUtil.initRedis(dataCacheUrl);
        
        //DataCacheUtil.getDataobjectListFromCache(0);
        
        Jedis jedis = null;
        long ret;
        
        while(true)
        {
            try {
                jedis = DataCacheUtil.jedisPool.getResource();  

                    DataCacheUtil.saveObjectToCache(jedis, "11111111", "2222222222222");
                    
                    ret = DataCacheUtil.getDataCacheSize();
                    
                    jedis.del("11111111");
                    
                    ret = DataCacheUtil.getDataCacheSize();
            }
            catch(Exception e)
            {
                if ( e instanceof JedisConnectionException )
                    System.out.printf("\n1111111111111 failed! e="+e);
            }
        }
  
  
    }

    /**
     * Test of saveObjectToCache method, of class DataCacheUtil.
     */
 @Ignore
    @Test
    public void testSaveObjectToCache() throws Exception 
    {
        System.out.println("saveObjectToCache");
        
        Util.commonServiceIPs = "127.0.0.1";
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
            
        String dataCacheUrl = Util.getSystemConfigValue(platformEm,0,"data_cache","data_cache_url");
        DataCacheUtil.initRedis(dataCacheUrl);
        long count = DataCacheUtil.getDataCacheSize();
            
        String dataobjectId = "1234567";
        
        Dataobject obj = new Dataobject();
        obj.setId(dataobjectId);
        obj.setChecksum("11111111111111111111111111");
        DataobjectBody dataobjectBody = new DataobjectBody();
        dataobjectBody.setDataobjectId(dataobjectId);
        dataobjectBody.setVersion(5);
        
        Jedis jedis = DataCacheUtil.jedisPool.getResource();  
        DataCacheUtil.saveObjectToCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT+obj.getId(), dataobjectBody);
        DataCacheUtil.saveObjectToCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT+obj.getId(), obj);
        
        count = DataCacheUtil.getDataCacheSize();
        
       // Dataobject dataobject1 = (Dataobject)DataCacheUtil.getObjectFromCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT+dataobjectId);
        DataobjectBody  dataobjectBody1 = (DataobjectBody)DataCacheUtil.getObjectFromCache(jedis, CommonKeys.DATA_CACHE_KEY_DATAOBJECT+dataobjectId);
          
        DataCacheUtil.removeDataobjectAndBodyFromCache(dataobjectId,"");
        
        count = DataCacheUtil.getDataCacheSize();
        
        DataCacheUtil.jedisPool.returnResource(jedis);  
        // TODO review the generated test code and remove the default call to fail.
   
    }

    
}
