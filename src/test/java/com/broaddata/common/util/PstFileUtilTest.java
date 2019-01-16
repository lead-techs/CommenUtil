/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
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
public class PstFileUtilTest {
    
    public PstFileUtilTest() {
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
     * Test of importPstFileToSystem method, of class PstFileUtil.
     */
    @Ignore
    @Test
    public void testImportPstFileToSystem() 
    {
  
        String filename = "D:/test/pst/1000MsgsPST.pst";

        DataServiceConnector dsConn;
        DataService.Client dataService;
        
        try
        {
            dsConn = new DataServiceConnector();
            dataService = dsConn.getClient("127.0.0.1");

            Map<String,Object> parameters = new HashMap<>();

            parameters.put("organizationId", 100);
            parameters.put("jobId",0);
            parameters.put("dataService",dataService);
            parameters.put("commonService",null);
            parameters.put("callback",null);
            parameters.put("alreadyProcessedLine",0);
            parameters.put("datasourceType",CommonKeys.FILE_DATASOURCE_TYPE);
            parameters.put("datasourceId",7);
            parameters.put("sourceApplicationId",1);
            parameters.put("targetRepositoryId",1);
            parameters.put("targetDataobjectType",CommonKeys.DATAOBJECT_TYPE_FOR_PST_FILE);

            Map<String, Object> importResults = PstFileUtil.importPstFileToSystem(filename,parameters);
        }
        catch(Exception e)
        {
            //fail("filed!");
        }         
    }
    
}
