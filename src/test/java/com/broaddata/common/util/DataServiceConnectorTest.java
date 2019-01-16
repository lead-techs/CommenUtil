/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DataServiceException;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author edf
 */
public class DataServiceConnectorTest {
        
    static final Logger log=Logger.getLogger("DataServiceConnectorTest");    
        
    public DataServiceConnectorTest() {
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

    /**
     * Test of getClient method, of class DataServiceConnector.
     */
       @Ignore
    @Test
    public void testGetClient() throws Exception {
        System.out.println("getClient");
        String dataServiceIPs = "192.168.100.41,192.168.100.42,192.168.100.47,192.168.100.43";
        DataServiceConnector conn = new DataServiceConnector();
    
        DataService.Client dataService = conn.getClient(dataServiceIPs);
        boolean isExists = false;
        int organizationId = 100;
        String dataobjectId = "32423";
        String indexName="234";
        int repositoryId = 1;
       
         while(true)
        {
            try
            {
                isExists = dataService.isDataobjectExistsInIndex(organizationId,dataobjectId, indexName, repositoryId);
            }
            catch(Exception e)
            {
                log.error("dataService.isDataobjectExistsInIndex() failed! e="+e);
                Tool.SleepAWhile(3, 0);

                if (e instanceof TTransportException )
                {
                    try 
                    {
                        conn.close();                           
                        dataService = conn.getClient(dataServiceIPs);
                    }
                    catch(Exception ex)
                    {
                        log.error("conn.getClient() failed! dataServiceIPs="+dataServiceIPs+"  e="+e);
                    }

                    continue;
                }
                else
                if ( e instanceof DataServiceException )
                {
                    isExists = false;
                    log.error(" DataServiceException! sleep 10,100 seconds, and retry!");
                }
            }

            break;
        }
                    
                    log.info("dataService.isDataobjectExistsInIndex() ="+isExists);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class DataServiceConnector.
     */
    @Ignore
    @Test
    public void testClose() {
        System.out.println("close");
        DataServiceConnector instance = new DataServiceConnector();
        instance.close();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
