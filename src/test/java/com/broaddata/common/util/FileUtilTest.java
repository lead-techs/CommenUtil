/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.DataValidationBehaviorType;
import com.broaddata.common.model.platform.StructuredFileDefinition;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
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
public class FileUtilTest {
    
    public FileUtilTest() {
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
     * Test of getFolderAllFiles method, of class FileUtil.
     */
   @Ignore
    @Test
    public void testProcessFile()
    {
        int organizationId = 100;
        String archiveFile = "D:\\test\\GAS_CUX_EA_CP_V_20170627_ADD_872.del";
        //CORE_BDFMHQAB_20150805_ADD_872
        //String archiveFile = "D:\\test\\CORE_BDFMHQAB_20150805_ADD_872.del";
        InputStream fileInputStream = FileUtil.getFileInputStream(archiveFile);
         
              Util.commonServiceIPs="127.0.0.1";
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        
        StructuredFileDefinition fileDefinition = platformEm.find(StructuredFileDefinition.class, 2007);
            
        boolean removeQuatation = true;
        
      //  StructuredDataFileUtil.importTxtFileToSystem(archiveFile,false,organizationId,0,
      //          DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue(),fileInputStream,0,1,true,
      //          0,null,null,fileDefinition,CommonKeys.FILE_DATASOURCE_TYPE, 0,0,0,removeQuatation,null,null,0,false,null);
        
        int i=0;
    }
    
    @Ignore
    @Test
    public void testSendFileToWebServer() throws Exception
    {
        String url = "http://localhost:8080/PlatformManager/upload";
        String filename = "D:/test/file/dddd.txt";
       
        FileUtil.sendFileToWebServer(url,filename,"text/plain");
               
    }
   @Ignore
    @Test
    public void test1() throws FileNotFoundException
    {
             
        String filename = "D:/test/gggggg.xlsx";
        File file = new File(filename);
        
        List<Map<String,Object>>  list = FileUtil.readExcelFileToMapList(filename, new FileInputStream(file), 2, true, 0,"Sheet1");
                
        int count = FileUtil.getExcelFileLine(filename,new FileInputStream(file),"");      
        
        System.out.println("count="+count);
    }

    //@Ignore
    @Test
    public void test()
    {
        int k=1;
        
        String filepath1 = "smb://192.168.2.5/public/test1/16.jpg";
        
        String filepath2 = "\\\\10.11.235.46\\doc\\Moore_052412.pdf";
        
        String aa="";
        byte[] bb= new byte[0];
        
        ByteBuffer test = ByteBuffer.wrap(bb);
        
        if ( test.capacity() == 0)
            k=0;
        
        ByteBuffer buf = FileUtil.readFileToByteBuffer(new File("\\192.168.1.17\\data\\share\\b3e53468-39dc-457e-bdbc-1950f412b3d5\\test1.mp4"));
        
        System.out.println(buf);
       // ByteBuffer buf = FileUtil.readSmbFileToByteBuffer(filepath);
        
    //   System.out.println(" buf="+buf.capacity());
    }
}
