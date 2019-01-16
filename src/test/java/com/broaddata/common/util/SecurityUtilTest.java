/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.util.SecurityUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author ef
 */
public class SecurityUtilTest {
    
    public SecurityUtilTest() {
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
    public void testEnryptPassword()
    {
       String userName = "system_admin";
       String password = "system_admin";
       
       String encryptedPassword = SecurityUtil.generatePasswordInSHA(userName, password);

    }
    /**
     * Test of encrypt method, of class SecurityUtil.
     */
    @Ignore
    @Test
    public void testEncrypt() throws Exception 
    {
                String content = "testsad d d * 的fasdfasdf 中国 afasdfsadf";
                String password = "12345678abcdefgh";
                
                String ret =  SecurityUtil.processEncyption(content ,password);
                
                
                //加密
                System.out.println("加密前：" + content);
                String encryptResultStr = SecurityUtil.encrypt(content, password);
 
                System.out.println("加密后：" + encryptResultStr);
              
                String result = SecurityUtil.decrypt(encryptResultStr,password);
                System.out.println("解密后：" + result);
       
                
                int i=0;
    }

  
    
}
