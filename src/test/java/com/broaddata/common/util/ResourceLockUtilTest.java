/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.LockResourceType;
import java.util.Map;
import javax.persistence.EntityManager;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author ed
 */
public class ResourceLockUtilTest {
    
    public ResourceLockUtilTest() {
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
    public void testCreateResourceLock() throws Exception 
    {
        System.out.println("createResourceLock");
        int organizationId = 100;
        String resourceId = "0";
        long version;
         
        String clientId = "node1";
        String otherInfo = "otherInfo";
        
        
        Util.commonServiceIPs="127.0.0.1";
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
              
        Client configEsClient = ESUtil.getClient(em, platformEm, 1, false);
         
        int lockResourceType = LockResourceType.DATA_CHANNEL_FOR_HBASE.getValue();
         
        version = ResourceLockUtil.updateLockInfo(configEsClient,organizationId,lockResourceType,resourceId,clientId,otherInfo);
        System.out.println(" update info version ="+version);
     
        
        for(int i=0;i<5;i++)
            ResourceLockUtil.createResourceLock(configEsClient, organizationId, lockResourceType, String.valueOf(i));
     
        version = 0;
       
        version = ResourceLockUtil.getResourceLock(configEsClient,organizationId,lockResourceType,resourceId,clientId,otherInfo);
        System.out.println(" get lock version ="+version);
       
        
        version = ResourceLockUtil.updateLockInfo(configEsClient,organizationId,lockResourceType,resourceId,clientId,otherInfo);
        System.out.println(" update info version ="+version);
     
        version = ResourceLockUtil.releaseLock(configEsClient,organizationId,lockResourceType,resourceId,clientId,otherInfo);
        System.out.println(" release info version ="+version);
    
    }
    
}
