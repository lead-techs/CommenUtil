/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.broaddata.common.util;

import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author fxy1949
 */
public class SEUtilTest {
    
    public SEUtilTest() {
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
     * Test of getWebPagesFromSearchEngine method, of class SEUtil.
     */
    @Ignore
    @Test
    public void testGetWebPagesFromSearchEngine() {
        System.out.println("getWebPagesFromSearchEngine");
        List<DiscoveredDataobjectInfo> dataInfoList = new ArrayList<>();
        int searchEngineInstanceId = 1;
        String keyword = "未来数据";
        int maxExpectedNumberOfPages = 10;
     //   SEUtil.getWebPagesFromSearchEngine(dataInfoList, searchEngineInstanceId, keyword, maxExpectedNumberOfPages);
        
        int k=0;
        // TODO review the generated test code and remove the default call to fail.
 
    }
    
}
