/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.manager.FTPUploader;
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
public class FTPUploaderTest {
    
    public FTPUploaderTest() {
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
     * Test of uploadFile method, of class FTPUploader.
     */
    @Ignore
    @Test
    public void testUploadFile() throws Exception 
    {
        System.out.println("Start");
        FTPUploader ftpUploader = new FTPUploader("119.23.130.153", "administrator", "Emmac2017");
        //FTP server path is relative. So if FTP account HOME directory is "/home/pankaj/public_html/" and you need to upload 
        // files to "/home/pankaj/public_html/wp-content/uploads/image2/", you should pass directory parameter as "/wp-content/uploads/image2/"
        
        ftpUploader.uploadFile("D:\\video\\img\\Image-0006-small.jpg", "Image-0006-small.png", "/aaaa/img/");
        
        ftpUploader.disconnect();
        
        System.out.println("Done");
    }

  
    
}
