/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.platform.DataItemStandard;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
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
public class DataItemStandardUtilTest {
    
    public DataItemStandardUtilTest() {
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
    public void testGetDataobjectTypesWithDataItemStandardId_4args() throws Exception 
    {
        int organizationId = 100;
 
        try
        {
            Util.commonServiceIPs="127.0.0.1";
            EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
                          
            int dataItemStandardId = 1;
            
            List<Map<String, Object>> result = DataItemStandardUtil.getDataobjectTypesWithDataItemStandardId(platformEm, organizationId,dataItemStandardId,true);
       
        }
        catch(Exception e)
        {
        
            throw e;
        }
   
    }

   
    
}
