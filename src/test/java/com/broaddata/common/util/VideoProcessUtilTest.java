/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.vo.PictureObjectData;
import java.awt.Color;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.persistence.EntityManager;
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
public class VideoProcessUtilTest {
    
    public VideoProcessUtilTest() {
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
  
    @Ignore
    @Test 
    public void testDuration()
    {
        String mediaFileUNCPath = "\\\\192.168.0.17\\data\\share\\5ccf00f0-aebc-4743-9bf1-ede6d94688b8\\c01_20180730140801.mp4";
        int d = VideoProcessUtil.getMediaDurationInSecond(mediaFileUNCPath);
        
        System.out.println(" d = "+d);
    }
    
    @Ignore
    @Test 
    public void callSSH() throws Exception
    {
        String videoServerHost = "192.168.1.70";
        String videoServerUser = "gpadmin";
        String videoServerPassword = "gpadmin";
        String databaseName = "stagingtestdb";
        String tableName = "test_1";
       
        String command = String.format("psql -p 5432 -U gpadmin -d %s -f /home/data/%s.sql \n",databaseName,tableName);
        Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,command,3,"");    
                
           fail("The test case is a prototype.");
    }
    
    @Ignore
    @Test
    public void drawObjectOnImage() throws Exception
    {
        int x= 200;
        int y = 300;
        int width = 300;
        int height = 200;
                
         
        ByteBuffer buf = FileUtil.readFileToByteBuffer(new File("D:\\video\\img\\Image-0001.jpg"));  
     
        
        Map<String,Object> info =  VideoProcessUtil.getImageInfo(buf);
        System.out.printf("width=%d", info.get("width"));
        System.out.printf("height=%d", info.get("height"));
        
        //buf = VideoProcessUtil.getSubImage(buf,x,y,width,height);    
        //FileUtil.writeFileFromByteBuffer("D:\\video\\img\\Image-0006-small.jpg", buf);
     
         info =  VideoProcessUtil.getImageInfo(buf);
        System.out.printf("width=%d", info.get("width"));
        System.out.printf("height=%d", info.get("height"));
                
        List<Map<String,Object>> objectInfoList = new ArrayList<>();
        Map<String,Object> objectInfo = new HashMap<>();
        
        objectInfo.put("label","2");
        objectInfo.put("x",500);
        objectInfo.put("y",500);
        objectInfo.put("width",300);
        objectInfo.put("height",200);
        
        objectInfoList.add(objectInfo);
        
        objectInfo = new HashMap<>();
        objectInfo.put("label","32");
        objectInfo.put("x",700);
        objectInfo.put("y",200);
        objectInfo.put("width",200);
        objectInfo.put("height",300);
        
        objectInfoList.add(objectInfo);        
         
        buf = VideoProcessUtil.drawObjectsOnImage(buf,objectInfoList);
        
        FileUtil.writeFileFromByteBuffer("D:\\video\\img\\Image-0006-3.jpg", buf);
        
        info =  VideoProcessUtil.getImageInfo(buf);
        System.out.printf(" width=", info.get("width"));
        System.out.printf(" height=", info.get("height"));
        
        String label = "2";
        
        buf = VideoProcessUtil.drawObjectOnImage(Color.YELLOW,null,label,buf,x,y,width,height);
        
        FileUtil.writeFileFromByteBuffer("D:\\video\\img\\Image-0006-1.jpg", buf);
        
        label = "12";
        
        buf = VideoProcessUtil.drawObjectOnImage(null,null,label,buf,x,y,width,height);
        
        FileUtil.writeFileFromByteBuffer("D:\\video\\img\\Image-0006-2.jpg", buf);
        
    }
    /**
     * Test of extractVedioObjects method, of class VedioProcessUtil.
     */
  @Ignore
    @Test
    public void test12222() throws Exception
    {
        String waiting = "Writing results file";
        
        while(true)
        {
            //String commandStr = "python3 /home/edf/devel/detectattrs.py -i /data/share/aaaa/img -objs /data/share/aaaa/object-detect-result.txt -o /data/share/aaaa/attributes_result.csv& \n";
   
            String commandStr = "nohup python3 /home/edf/devel/detectattrs.py -i /data/share/aaaa/img -objs /data/share/aaaa/object-detect-result.txt -o /data/share/aaaa/attributes_result.csv >output.out 2>error.out </dev/null& \n";
   
            Tool.executeSSHCommandChannelShell("192.168.1.17",22,"edf","edf",commandStr,1,"");
            
            Tool.SleepAWhile(3, 0);
        }
       
    }
    
@Ignore
    @Test
    public void testExtractVedioObjects() throws Exception 
    {
        System.out.println("extractVedioObjects");
    
        int modelId = 0;
        //String videoServerWorkingDirectory = String.format("/edf/video/%s",UUID.randomUUID().toString());
        String videoServerWorkingDirectory = String.format("/edf/video/%s","aaaa-bbbb");
        String edfServerWorkingDirectory = String.format("E:/video/%s","aaaa-bbbb");
        String videoFilepathInVedioServer = String.format("%s/test1.mp4",videoServerWorkingDirectory);
        List<PictureObjectData> expResult = null;
               
        // call video processor
        //String videoServerHost = "192.168.1.17";
        //String videoServerUser = "edf";
        //String videoServerPassword = "edf";
        
        Util.commonServiceIPs="127.0.0.1";
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
                 
        Map<String,String> map = Util.getEDFVideoProcessingService(platformEm,"14");
        
        String videoServerHost = Util.pickOneServiceIP(map.get("ips"));
        String videoServerUser = map.get("user");
        String videoServerPassword = map.get("password");
        
       //List<PictureObjectData> result = VideoProcessUtil.extractVideoObjectFeatures(videoServerHost,videoServerUser,videoServerPassword,videoServerWorkingDirectory,edfServerWorkingDirectory,modelId,videoFilepathInVedioServer);
        
        fail("The test case is a prototype.");
    }
 
}
