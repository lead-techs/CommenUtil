
package com.broaddata.common;

import com.broaddata.common.util.OSSUtil;
import com.aliyun.openservices.oss.model.ObjectMetadata;
import org.junit.Ignore;
import com.aliyun.openservices.oss.OSSClient;
import java.io.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author fangf
 */
public class OSSUtilTest 
{
    private static OSSClient client;
    private static final String OSS_ENDPOINT = "http://oss.aliyuncs.com/";
    private static final String access_id = "R9Hzm6APgovT81kR";
    private static final String access_key = "ha2Xx0XIZtS7qO55Fi25vp3itYgPlx";
    private static final String bucketName = "frank-test";
    
    public OSSUtilTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception 
    {
        // client = OSSUtil.getClient(OSS_ENDPOINT, access_id, access_key);
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
     * Test of getClient method, of class OSSUtil.
     */
    @Ignore
    @Test
    public void testGetClient() throws Exception 
    {
        System.out.println("getClient");

        client = OSSUtil.getClient(OSS_ENDPOINT, access_id, access_key);
 
        assertNotNull(client);
    }

    /**
     * Test of storeDocument method, of class OSSUtil.
     */
    @Ignore
    @Test
    public void testUploadFile() throws Exception 
    {
        System.out.println("uploadFile");
        String key = "app1/pic/ttt.txt";
        String filename = "c:/test/ttt.txt";
        
      //  client.doesBucketExist(bucketName);
        OSSUtil.storeDataobject(client, bucketName, key, filename, new ObjectMetadata());
    }


    /**
     * Test of downloadFile method, of class OSSUtil.
     */

    @Ignore
    @Test
    public void testDownloadFile() throws Exception {
        System.out.println("downloadFile");
        String key = "14.JPG";
        String filename = "c:/test/14.jpg";
        OSSUtil.getDataobject(client, "frank-test", key, filename);
    }

    /**
     * Test of uploadBigFile method, of class OSSUtil.
     */
    @Ignore
    @Test
    public void testUploadBigFile() throws Exception {
        System.out.println("uploadBigFile");
        String bucketName = "frank-test";
        String key = "Wildlife.wmv";
        File uploadFile = new File("c:/test/aaa.rar");
        OSSUtil.storeBigDataobject(client, bucketName, key, uploadFile);
    }

    /**
     * Test of storeDocument method, of class OSSUtil.
     */
    @Ignore    
    @Test
    public void testUploadFile_4args_1() throws Exception {
        System.out.println("uploadFile");
        OSSClient client = null;
        String bucketName = "";
        String key = "";
        String filename = "";
        OSSUtil.storeDataobject(client, bucketName, key, filename, new ObjectMetadata());
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of storeDocument method, of class OSSUtil.
     */
    @Ignore     
    @Test
    public void testUploadFile_4args_2() throws Exception {
        System.out.println("uploadFile");
        OSSClient client = null;
        String bucketName = "";
        String key = "";
        byte[] fileStream = null;
        OSSUtil.storeDataobject(client, bucketName, key, fileStream, new ObjectMetadata());
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}
