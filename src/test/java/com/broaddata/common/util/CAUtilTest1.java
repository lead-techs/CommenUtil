/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Date;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author fangf
 */
public class CAUtilTest1 {
    
    public CAUtilTest1() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of extractText method, of class CAUtil.
     */
     
    @Ignore
    @Test
    public void testExtractText() {
        System.out.println("extractText");
        
        String filename = "D:/test/嘉兴市局大数据平台服务器信息配置.xlsx"; 
    //    String filename = "c:/test/test1/Car-speakers-590x90.swf";
       // byte[] fileStream = FileUtil.readFileToByteArray(new File("c:/test/test1/test333.txt"));
        //byte[] fileStream = FileUtil.readFileToByteArray(new File("c:/test/test1/aaa.pdf"));     
        ByteBuffer fileStream = FileUtil.readFileToByteBuffer(new File(filename));  
        
        String mime_type = CAUtil.detectMimeType(fileStream);
        String encoding = CAUtil.detectEncoding(fileStream);   
                
        System.out.print(new Date());
        String result1 = CAUtil.extractText(fileStream,mime_type);
        System.out.print(new Date());
     
        
        System.out.print(new Date());
        String result = CAUtil.extractText(fileStream);
        System.out.print(new Date());
 

        System.out.print(result);
        System.out.print(result1);
    }

    /**
     * Test of getLangue method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testGetLangue() {
        
        System.out.println("getLangue");
             //   byte[] fileStream = FileUtil.readFileToByteArray("c:\\test\\persistence.xml");
       // byte[] fileStream = FileUtil.readFileToByteArray("c:\\test\\Moore_052412.pdf");
        //byte[] fileStream = FileUtil.readFileToByteArray("c:\\test\\1 - Beyond Coding.pptx");
        //byte[] fileStream = FileUtil.readFileToByteArray("c:\\test\\测试.txt");                
        String expResult = "xxx";
//        String result = CAUtil.getMimeType("c:\\test\\Moore_052412.pdf");
        String result = CAUtil.detectEncoding("c:\\test\\Moore_052412.pdf");
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of detectMimeType method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testDetectMimeType_byteArr() {
        System.out.println("detectMimeType");
        byte[] fileStream = null;
        String expResult = "";
        String result = CAUtil.detectMimeType(fileStream);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of detectMimeType method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testDetectMimeType_String() {
        System.out.println("detectMimeType");
        String fileName = "";
        String expResult = "";
        String result = CAUtil.detectMimeType(fileName);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of detectMimeType method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testDetectMimeType_ByteBuffer() {
        System.out.println("detectMimeType");
        ByteBuffer fileStream = null;
        String expResult = "";
        String result = CAUtil.detectMimeType(fileStream);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of extractText method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testExtractText_ByteBuffer() {
        System.out.println("extractText");
        ByteBuffer fileStream = null;
        String expResult = "";
        String result = CAUtil.extractText(fileStream);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of extractText method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testExtractText_byteArr() {
        System.out.println("extractText");
        byte[] fileStream = null;
        String expResult = "";
        String result = CAUtil.extractText(fileStream);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of detectEncoding method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testDetectEncoding_ByteBuffer() {
        System.out.println("detectEncoding");
        ByteBuffer fileStream = null;
        String expResult = "";
        String result = CAUtil.detectEncoding(fileStream);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of detectEncoding method, of class CAUtil.
     */
    @Ignore
    @Test
    public void testDetectEncoding_byteArr() {
        System.out.println("detectEncoding");
        byte[] fileStream = null;
        String expResult = "";
        String result = CAUtil.detectEncoding(fileStream);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of detectEncoding method, of class CAUtil.
     */
   @Ignore
    @Test
    public void testDetectEncoding_String() {
        System.out.println("detectEncoding");
        String filename = "c:/test/test1/aaa.pdf";
        
        ByteBuffer buf = FileUtil.readFileToByteBuffer(new File(filename));
        
        String expResult = "";
        String result = CAUtil.detectEncoding(buf);
    
    }
}
