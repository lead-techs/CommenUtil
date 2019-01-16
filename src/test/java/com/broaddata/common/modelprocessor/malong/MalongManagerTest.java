/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.modelprocessor.malong;

import cn.productai.api.pai.entity.dressing.DressingClassifyResponse;
import java.io.File;
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
public class MalongManagerTest {
    
    public MalongManagerTest() {
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
     * Test of invokeDressingClassifying method, of class MalongManager.
     */
    
    @Ignore
    @Test
    public void testInvokeDressingClassifying() throws Exception 
    {
        System.out.println("invokeDressingClassifying");
        
        String keyId="6b4d91154cba26e4c82dc1c95052e715";
        String secretKey = "aad069f6e5b4eb5f81792bb4fd5ee367";
        String version = "1";
        String host = "api.productai.cn";
             
        MalongManager manager = new MalongManager(host,keyId,secretKey,version);
        
        manager.init();
        
        DressingClassifyResponse expResult = null;
        String imageUrl="http://119.23.130.153/aaaa/img/Image-0006.jpg";
        
      //  DressingClassifyResponse result = manager.invokeDressingClassifyingByUrl(imageUrl,0);
        
     //   System.out.printf(" result="+result);
         
    }

    /**
     * Test of init method, of class MalongManager.
     */
    @Ignore
    @Test
    public void testInit() {
        System.out.println("init");
        MalongManager instance = null;
        instance.init();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of saveImageToDataset method, of class MalongManager.
     */
        @Ignore
    @Test
    public void testSaveImageToDataset() throws Exception 
    {
   System.out.println("invokeDressingClassifying");
        
        String keyId="6b4d91154cba26e4c82dc1c95052e715";
        String secretKey = "aad069f6e5b4eb5f81792bb4fd5ee367";
        String version = "1";
        String host = "api.productai.cn";
             
        MalongManager manager = new MalongManager(host,keyId,secretKey,version);
        
        manager.init();
         
        String imageUrl="http://119.23.130.153/aaaa/img/Image-0006.jpg";
        
        String newDatasetId = manager.saveImageToImageSet(imageUrl, "","dataset-1");
        
        manager.saveImageToImageSet(imageUrl, newDatasetId,"dataset-1");
    }

    /**
     * Test of convertMalongBoxValueToStandard method, of class MalongManager.
     */
    @Ignore
    @Test
    public void testConvertMalongBoxValueToStandard() {
        System.out.println("convertMalongBoxValueToStandard");
        double boxValue = 0.0;
        int total = 0;
        MalongManager instance = null;
        int expResult = 0;
        int result = instance.convertMalongBoxValueToStandard(boxValue, total);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of invokeDressingClassifyingByFile method, of class MalongManager.
     */
    @Ignore
    @Test
    public void testInvokeDressingClassifyingByFile() throws Exception {
        System.out.println("invokeDressingClassifyingByFile");
        File imageFile = null;
        int retryTime = 0;
        MalongManager instance = null;
        DressingClassifyResponse expResult = null;
        DressingClassifyResponse result = instance.invokeDressingClassifyingByFile(imageFile, retryTime);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of invokeDressingClassifyingByUrl method, of class MalongManager.
     */
    @Ignore
    @Test
    public void testInvokeDressingClassifyingByUrl() throws Exception {
        System.out.println("invokeDressingClassifyingByUrl");
        String imageUrl = "";
        int retryTime = 0;
        MalongManager instance = null;
        DressingClassifyResponse expResult = null;
        DressingClassifyResponse result = instance.invokeDressingClassifyingByUrl(imageUrl, retryTime);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
