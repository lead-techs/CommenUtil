/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.broaddata.common.util;

import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.FileUtil;
import java.io.File;
import java.nio.ByteBuffer;
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
public class DataIdentifierTest {
    
    public DataIdentifierTest() {
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
    
    @Test
    public void testGenerateChecksum() {
     
        String str = "2;2014-03-12 12:13:33.0;1;12;AAAAA;123.34;purchase;";
        String dataobjectId="84B50905A0BF0000F3AD99314730D997E7525298";
        
        
        String checksum = DataIdentifier.generateDataobjectChecksum(100, dataobjectId, str);
        System.out.print("1checksum="+checksum+"\n");
        
        checksum = DataIdentifier.generateDataobjectChecksum(100, dataobjectId, str);
        System.out.print("2checksum="+checksum+"\n");
        
        checksum = DataIdentifier.generateDataobjectChecksum(100, dataobjectId, str);
        System.out.print("3checksum="+checksum+"\n");        
        
    }

    /**
     * Test of generateContentId method, of class DataIdentifier.
     */
 @Ignore
    @Test
    public void testGenerateContentId() {
        System.out.println("generateContentId");
       // ByteBuffer documentContent = FileUtil.readFileToByteBuffer(new File("c:/test/test1/花絮.avi"));
        byte[] documentContent = FileUtil.readFileToByteArray(new File("c:/test/test1/花絮.avi"));
        String expResult = "E24FDB431F777BBAB97142562534490B7AE14062";
       
        String result = DataIdentifier.generateContentId(documentContent);
        assertEquals(expResult, result);

    }

    /**
     * Test of generateDataobjectId method, of class DataIdentifier.
     */
    @Ignore
    @Test
    public void testGenerateDataobjectId() {
        System.out.println("generateDataobjectId");
        int organizationId = 0;
        int sourceApplicationId = 0;
        int datasourceType = 0;
        int datasourceId = 0;
        String fullpathName = "";
        String expResult = "";
        String result = DataIdentifier.generateDataobjectId(organizationId, sourceApplicationId, datasourceType, datasourceId, fullpathName,1);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
