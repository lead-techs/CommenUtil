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
public class CAUtilTest {
    
    public CAUtilTest() {
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
}
