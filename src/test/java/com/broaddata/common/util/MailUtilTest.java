/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.broaddata.common.util;

import com.broaddata.common.util.MailUtil;
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
 * @author ef
 */
public class MailUtilTest {
    
    public MailUtilTest() {
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
     * Test of sendMail method, of class MailUtil.
     */
    
    @Ignore
    @Test
    public void testSendMail() throws Exception {
        System.out.println("sendMail");
        String to = "fxy1949@hotmail.com";
        String from = "fxy1949@hotmail.com";
        String subject = "test subject111";
        String body = "test body111";
        List<String>  files = new ArrayList<>();
        files.add("C:\\test\\file\\aaa.txt");
        MailUtil.sendMail(to, from, "fxy1949@hotmail.com", "Csz781224",subject, body, files);
        // TODO review the generated test code and remove the default call to fail.
       // fail("The test case is a prototype.");
    }
    
}
