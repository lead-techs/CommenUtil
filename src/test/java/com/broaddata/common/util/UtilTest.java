/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.util.CAUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.ServiceGuardConnector;
import com.broaddata.common.util.CommonServiceConnector;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.model.enumeration.ComputingNodeService;
import com.broaddata.common.model.enumeration.JobTimeType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.TimeUnit;
import com.broaddata.common.model.organization.Content;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.Metadata;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.model.pojo.DataWorkerAdminData;
import com.broaddata.common.processor.Processor;

import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.commonservice.CommonService.Client;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.thrift.serviceguard.ServiceGuard;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Query;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.dom4j.Document;
import org.elasticsearch.action.update.UpdateResponse;
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
public class UtilTest {
    
    public UtilTest() {
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
    
   // @Ignore
     @Test
    public void testRegularExpression()
    {
        boolean matched;
        
        String regularExpression = "^[A-Za-z0-9]+$";
        String value = "azbc1233BBB";
        
        Pattern pattern  = Pattern.compile(regularExpression);
        matched = pattern.matcher(value).matches();
                     
        regularExpression = "^\\d{15}|\\d{18}$";
        value = "123456789012345123";
        
        pattern  = Pattern.compile(regularExpression);
        matched = pattern.matcher(value).matches();
    }
    
 @Ignore
 @Test
    public void testA()  throws Exception
    {
         
        String file = "D:/test/tmp/EdfDataBatchTask-1.0.jar";
        DataBatchProgramBase dataBatchProgramBase = (DataBatchProgramBase)Util.getDataBatchProgramBaseFromFile(file,"com.broaddata.edfdatabatchtask.DataBatchJobExample"); 
        
        int i=0;
    }
    
    @Ignore
    @Test
    public void testAAA() throws Exception
    {
        Util.commonServiceIPs="127.0.0.1";
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
            
        DatasourceConnection dc = em.find(DatasourceConnection.class, 26);
         
        List<String> list = JDBCUtil.getDatabaseStoredProcedureNameList(dc,"");
            
        String sqlStr = "111 INSERT INTO";
        String sql = sqlStr.substring(sqlStr.indexOf(" ")+1);
                                    
        String dataobjectTypeIdStr = sqlStr.substring(0,sqlStr.indexOf(" "));
        int    dataobjectTypeId = Integer.parseInt(dataobjectTypeIdStr);
        //  updateSql = String.format("UPDATE %s SET ",tableName);"INSERT INTO %s ( %s ) VALUES (",tableName,columnNameStr);
        sqlStr = "INSERT INTO AAAA ( BBBB,CCCC ) VALUES (";
        String aaa = Tool.getTableNameFromSQLStr(sqlStr);
        
        sqlStr = "UPDATE AAAA SET ssss";
        aaa = Tool.getTableNameFromSQLStr(sqlStr);
        
        int i= 0;
    }
   
    @Ignore
    @Test
    public void testGetdataobjectTypeInfo() throws Exception
    {
  
        int organizationId = 100;
        int repositoryId = 1;
        
        try
        {
            Util.commonServiceIPs="127.0.0.1";
            EntityManager em = Util.getEntityManagerFactory(organizationId).createEntityManager();
            EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
            
            String[] parameters = "getCountValueFromES,1,CORE_BDFMHQAC_type".split("\\,");
            String info = Util.getDataobjectTypeOtherInfo(organizationId,em, platformEm,parameters);
   
            System.out.println(" info="+info);
        }
        catch(Exception e)
        {
        
            throw e;
        }
      
    }
    
    
    @Ignore
    @Test
    public void testSpeekRecognition() throws Exception
    {
        //String filename = "c:\\test\\file\\vedio\\new1.wmv";
       // MediaDocumentSpeekRecognizer recognizer = new MediaDocumentSpeekRecognizer();
        
       // String text = recognizer.recognize(filename);
        
      //  System.out.print(text);
        int organizationId = 100;
        int repositoryId = 1;
        
        try
        {
            Util.commonServiceIPs="127.0.0.1";
            EntityManager em = Util.getEntityManagerFactory(organizationId).createEntityManager();
            EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
            
          /*  org.elasticsearch.client.Client esClient = ESUtil.getClient(em,platformEm,repositoryId,false);
   
            String queryStr = String.format("_id:%s","70F7B0895B0CE4CA18113C721121D89BEA3B5824"); // 101004599691531

            /*Map<String,Object> searchRet = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, new String[]{},
                            queryStr, "", 1, 0, new String[]{"tags"}, new String[]{}, new String[]{},null);

            int currentHits = (Integer) searchRet.get("currentHits");

            List<Map<String, Object>> searchResults = (List<Map<String, Object>>) searchRet.get("searchResults");

            for (Map<String, Object> result : searchResults) 
            {
                String dataobjectId = (String) result.get("id");
                dataobjectId = dataobjectId.substring(0, 40);
                String indexName = (String) result.get("index");
                String indexType = (String)result.get("indexType");

                Map<String,Object> fieldMap = (Map<String,Object>)result.get("fields");

                Object obj = fieldMap.get("tags");
                List<String> tags = new ArrayList<>();

                if ( obj instanceof Collection )
                    tags = (List<String>)obj;
                else
                    tags.add((String)obj);

                fieldMap = new HashMap<>();

                String newTag = "bal_type~14";
            
                List<String> newTags = new ArrayList<>();

                boolean found = false;
                for(String tag : tags)
                {
                    if ( tag.startsWith(newTag.substring(0, newTag.indexOf("~"))) )
                    {
                        found = true;
                        newTags.add(newTag);
                    }
                    else
                        newTags.add(tag);
                }                    
                
                if ( !found )
                    newTags.add(newTag);

                fieldMap.put("tags",newTags);

                UpdateResponse response = esClient.prepareUpdate().setIndex(indexName).setType(indexType).setId(dataobjectId).setDoc(fieldMap).get();
            }*/

        }
        catch(Exception e)
        {
        
            throw e;
        }
      
    }
    
    
    @Ignore
    @Test
    public void test444() throws ScriptException
    {
        //String groovyScript = "num1*0.2+ num2*0.4-5";
        
        String groovyScript1 = "  list.each { println it}";
        String groovyScript2 = " return map.row1.col1.toInteger()+map.row1.col2.toInteger()";
 
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("groovy");
        
        String[] list = new String[]{"aa","bb","cc"};
        engine.put("list",list);
     
       
        Object value = engine.eval(groovyScript1);
       
           
        Map<String,Map<String,String>> map = new HashMap<>();
        Map<String,String> m = new HashMap<>();
        m.put("col1","11");
        m.put("col2","12");
        
        map.put("row1",m);
        
        m = new HashMap<>();
        m.put("col1","222cValue1");
        m.put("col2","222cValue2");
        
        map.put("row2",m);
        engine.put("map",map);
        
        value = engine.eval(groovyScript2);
        
        System.out.print(value);
    }
    @Ignore
    @Test
    public void test333() throws ScriptException
    {
        //String groovyScript = "num1*0.2+ num2*0.4-5";
        
        String groovyScript = "  if ( num_3>10 ) "
                + "                return 1.0 "
                + "              else"
                + "              if ( num_2<3 )"
                + "                return 2.0"
                + "               else"
                + "                return 3.0";
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("groovy");
        
        engine.put("num_1",11);
        engine.put("num_2",5);
       
        Object value = engine.eval(groovyScript);
        
        engine.put("num_1",10);
        engine.put("num_2",5);
       
        value = engine.eval(groovyScript);
        
        System.out.print(value);
    }
    
   @Ignore
    @Test
    public void test222()
    {
        String groovyScript = "num1*0.2+ num2*0.4-5";
        
        Binding binding = new Binding();
        binding.setVariable("num1", 10);
        binding.setVariable("num2", 2);
        
        GroovyShell shell = new GroovyShell(binding);
        
        Object value = shell.evaluate(groovyScript);
       
        System.out.print(value);
    }
    
    @Ignore
    @Test 
    public void test11()
    {
        String localFolder = "C:\\test\\jiashan\\data";
        String archiveFile;
        String encoding;
        
        List<DiscoveredDataobjectInfo> dataInfoList = new ArrayList<>();         
        
        try 
        {
            FileUtil.getFolderAllFiles(localFolder,dataInfoList,null,true,false, 0, 0,null, 0, null, null); 

            for(DiscoveredDataobjectInfo info : dataInfoList )
            {
                archiveFile = localFolder + "\\"+ info.getName();

                //encoding = CAUtil.detectEncoding(archiveFile);
                encoding="GBK";

                System.out.printf("archive file=%s encode=%s\n", archiveFile,encoding);

                File file = new File(archiveFile);

                InputStream in = new FileInputStream(file);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in,encoding));

                String line = reader.readLine();

                System.out.printf("line=%s\n", line);
            }            
        }
        catch(Exception e)
        {
           fail("failed!");   
        }
       
    }
    
     @Ignore
    @Test 
    public void test12()
    {
          Pattern pattern  = Pattern.compile("^GAS_CUX_FND_BRANCH_ORG_MAPPING_[0-9]{8}_\\w*.del$");
          boolean matched = pattern.matcher("GAS_CUX_FND_BRANCH_ORG_MAPPING_20150802_ALL_872.del").matches();
        
                
    /*    String line ="\"10100002,1169294\",\"2015-08-05\", 02879829., 00000003., 9829.,,\"00000,0D5B61L，曹叶婷,赔款            \",\"722210\"";
        
 
        String test = Util.removeUnExpectedChar(line, ",", "#");

        System.out.print(line+"\n");
        System.out.print(test+"\n");*/
    }
    
    @Ignore
    @Test 
    public void test1()
    {
        String value = "2015/04/23 11:11:33";
        
        MetadataDataType type = Util.guessDataType(value);
 
        return;
    }
    
    @Ignore
    @Test
    public void test()
    {
        Date date = new Date();
        String dateStr = Tool.convertDateToTimestampStringForES(date);
                
        String reg = "custname\\s.*?\\s,2,custname";
        String[] parameter=reg.split("\\,");
        String fieldValue;
        String newStr;
        String logContentStr = "10031664 custname 魏殿忠 orgid 6201 custname 魏殿忠 bankcode custname 魏殿忠 6006 time";
         Pattern pattern  = Pattern.compile(parameter[0]);

        Matcher m = pattern.matcher(logContentStr);
        boolean found = false;

        for(int i=0;i<Integer.parseInt(parameter[1]);i++)
        {
            found = m.find(); 
            if ( !found )
                break;
        }

        if ( found )
        {
            fieldValue = m.group(); 
            fieldValue = fieldValue.replaceAll(parameter[2], "").trim();

            int i = m.start();

            newStr = logContentStr.substring(0,m.start())+logContentStr.substring(m.start()).replaceFirst(fieldValue,"<span style=\"color:red\">["+fieldValue+"]</span>");

            int k=0;
        }
                        
        /*
        Date date = new Date();
        
        Date newDate = Tool.changeToGmt(date);
        
        newDate = Tool.changeToLocal(newDate);
        
        long itemNumber = 0;
        long timeDiff = 17;
        itemNumber =  17%24;
        
        
         String str="aaaa bbbbbbbbbbb ccccccccc";
         List<String> ret = Tool.getSplitedWords(str);
         
         str="\"aaaa bbbbbbbbbbb\" AND ccccccccc \"ccc ddd eeee\" bb";
         ret = Tool.getSplitedWords(str);
         
         return;
         Map<String,Date> map =  Util.getRelativeTimePeriod(10,TimeUnit.DAY.getValue());
         
         System.out.print(map);
         
  map =  Util.getRelativeTimePeriod(2,TimeUnit.MONTH.getValue());
         
         System.out.print(map);*/
    }
    
    @Ignore
    @Test
    public void testContent() throws Exception 
    {
        try 
        {
            ServiceInstance objectStorageServiceInstance = null;
            String objectStoreServiceProvider;
            Map<String,String> objectStoreServiceProviderProperties;

            String filename = "c:/test/test1/aaa.pdf";
            //String filename = "c:/test/test1/big.wmv";

            //RuntimeContext.computingNode = 
                    
            ByteBuffer fileStream = FileUtil.readFileToByteBuffer(new File(filename));

            String encoding = CAUtil.detectEncoding(fileStream);
            String mimeType = CAUtil.detectMimeType(fileStream);
            String text = CAUtil.extractText(fileStream);

            CommonServiceConnector csConn = new CommonServiceConnector();
            CommonService.Client commonService = csConn.getClient("127.0.0.1");

            objectStorageServiceInstance = (ServiceInstance)Tool.deserializeObject(commonService.getObjStorageSI(100,1).array());
            objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
            objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());

          //  ContentStoreUtil.storeContent(RuntimeContext.computingNode,"12345678",fileStream,objectStorageServiceInstance.getId(),objectStoreServiceProvider,objectStoreServiceProviderProperties);            
        }
        catch (Exception e)
        {
            System.out.println(" e="+e);
        }        
    }
    
    @Ignore
    @Test 
    public void testServiceGuard()
    {
        ServiceGuardConnector sgConn = null;
        ServiceGuard.Client serviceGuard = null;
        
        try 
        {
            // connect data service server
            int nodeId = 1;
            sgConn = new ServiceGuardConnector();
            serviceGuard = sgConn.getClient("127.0.0.1");

            serviceGuard.startService(nodeId,ComputingNodeService.DATA_SERVICE.getValue(), null);       
           // serviceGuard.startService(nodeId,ComputingNodeService.SEARCH_SERVICE.getValue(), null);   
                       
            List<Integer> serviceIds = new ArrayList<>();
            serviceIds.add(3);
            serviceIds.add(5);
            
            List<Map<String,String>> serviceStatusList = serviceGuard.getServiceStatus(nodeId,serviceIds,null);
                        
           // serviceGuard.stopService(nodeId,ComputingNodeService.SEARCH_SERVICE.getValue(),null); 
            
            serviceGuard.stopService(nodeId,ComputingNodeService.DATA_SERVICE.getValue(), null);         
            
            assertEquals(0, 0);
        }
        catch (Exception e)
        {
            System.out.println(" e="+e);
            e.printStackTrace();
            fail("The test case is a prototype.");
        }
        finally
        {
            sgConn.close();
        }
    }
  
    /**
     * Test of getOrganizations method, of class Util.
     */
     @Ignore
    @Test
    public void testGetOrganizations() {
        System.out.println("getOrganizations");
        int platformInstanceId = 1;
        int[] expResult = null;
        //int[] result = Util.getDataserviceInstanceOrganizations(platformInstanceId);
        //assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

   

    /**
     * Test of getDataserviceInstanceId method, of class Util.
     */
    @Ignore
    @Test
    public void testGetDataserviceInstanceId() {
        System.out.println("getDataserviceInstanceId");
        int computingNodeId = 1;
        int expResult = 1;
        //int result = Util.getDataserviceInstanceId(computingNodeId);
       // assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getSystemConfigValue method, of class Util.
     */
    @Ignore
    @Test
    public void testGetSystemConfigValue() throws Exception {
        System.out.println("getSystemConfigValue");
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        int organizationId = 0;
        String key = "data_processing";
        String name = "job_working_area_location";
        String expResult = "C:/tmp";
        String result = Util.getSystemConfigValue(platformEm, organizationId, key, name);
        platformEm.close();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSystemConfigValueList method, of class Util.
     */
    @Ignore
    @Test
    public void testGetSystemConfigValueList() throws Exception {
        System.out.println("getSystemConfigValueList");
        EntityManager platformEm = null;
        int organizationId = 0;
        String key = "";
        List<String> expResult = null;
        List<String> result = Util.getSystemConfigValueList(platformEm, organizationId, key);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }   

    /**
     * Test of getRepositorySearchEngineIP method, of class Util.
     */
    @Ignore
    @Test
    public void testGetRepositorySearchEngineIP() {
        System.out.println("getRepositorySearchEngineIP");
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        int repositoryId = 1;
        List<String> expResult = null;
        List<String> result = Util.getRepositorySearchEngineIP(em, platformEm, repositoryId);
        //assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
       
    }

    /**
     * Test of sortListAccordingToScore method, of class Util.
     */
    @Ignore    
    @Test
    public void testSortListAccordingToScore() {
        System.out.println("sortListAccordingToScore");
        List<Map<String, Object>> list = null;
        Util.sortListAccordingToScore(list);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getServiceGuardPort method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetServiceGuardPort() {
        System.out.println("getServiceGuardPort");
        String nodeType = "";
        int expResult = 0;
        int result = Util.getServiceGuardPort(nodeType);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of convertToSystemDataType method, of class Util.
     */
    @Ignore    
    @Test
    public void testConvertToSystemDataType() throws Exception {
        System.out.println("convertToSystemDataType");
        int sqlDataType = 0;
        int expResult = 0;
        int result = Util.convertToSystemDataType(String.valueOf(sqlDataType),"0","0");
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getNameByIdInEnum method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetNameByIdInEnum() {
        System.out.println("getNameByIdInEnum");
        String enumName = "";
        int id = 0;
        String expResult = "";
        String result = Util.getNameByIdInEnum(enumName, id);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of DeleteDataWorkerAdminData method, of class Util.
     */
    @Ignore    
    @Test
    public void testDeleteDataWorkerAdminData() throws Exception {
        System.out.println("DeleteDataWorkerAdminData");
        Util.DeleteDataWorkerAdminData();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDataWorkerAdminData method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDataWorkerAdminData() throws Exception {
        System.out.println("getDataWorkerAdminData");
        DataWorkerAdminData expResult = null;
        DataWorkerAdminData result = Util.getDataWorkerAdminData();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDatasourceType method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDatasourceType() {
        System.out.println("getDatasourceType");
        int datasourceType = 0;
        List<DatasourceType> datasourceTypes = null;
        DatasourceType expResult = null;
        DatasourceType result = Util.getDatasourceType(datasourceType, datasourceTypes);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDataobjectContent method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDataobjectContent() {
        System.out.println("getDataobjectContent");
        int storageServiceInstanceId = 0;
        String contentId = "";
        byte[] expResult = null;
        byte[] result = Util.getDataobjectContent(storageServiceInstanceId, contentId);
        assertArrayEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of findSingleValueMetadata method, of class Util.
     */
    @Ignore    
    @Test
    public void testFindSingleValueMetadata() {
        System.out.println("findSingleValueMetadata");
        String name = "";
        //List<FrontEndMetadata> metadatas = null;
        String dataobjectOrContentId = "";
        int version = 0;
        String expResult = "";
       // String result = Util.findSingleValueMetadataValue(dataobjectOrContentId, version,name, metadatas);
      //  assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of findMutiValueMetadataValue method, of class Util.
     */
    @Ignore    
    @Test
    public void testFindMutiValueMetadataValue() {
        System.out.println("findMutiValueMetadataValue");
        String name = "";
        List<Metadata> metadatas = null;
        String dataobjectOrContentId = "";
        int version = 0;
        List<String> expResult = null;
       // List<String> result = Util.findMutiValueMetadataValue(dataobjectOrContentId, version,name, metadatas);
       // assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDataobjectType method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDataobjectType() {
        System.out.println("getDataobjectType");
        int dataobjectTypeId = 0;
        DataobjectType expResult = null;
        //DataobjectType result = Util.getDataobjectType(dataobjectTypeId);
        //assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDatasourceTypesFromClient method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDatasourceTypesFromClient() {
        System.out.println("getDatasourceTypesFromClient");
        Client commonService = null;
        List<DatasourceType> expResult = null;
        List<DatasourceType> result = Util.getDatasourceTypesFromClient(commonService);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDataobjectTypesFromClient method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDataobjectTypesFromClient() {
        System.out.println("getDataobjectTypesFromClient");
        Client commonService = null;
        List<DataobjectType> expResult = null;
        List<DataobjectType> result = Util.getDataobjectTypesFromClient(commonService);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getEntityManagerFactory method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetEntityManagerFactory() {
        System.out.println("getEntityManagerFactory");
        int organizationId = 0;
        EntityManagerFactory expResult = null;
        EntityManagerFactory result = Util.getEntityManagerFactory(organizationId);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getPlatformEntityManagerFactory method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetPlatformEntityManagerFactory() {
        System.out.println("getPlatformEntityManagerFactory");
        EntityManagerFactory expResult = null;
        EntityManagerFactory result = Util.getPlatformEntityManagerFactory();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDataserviceInstanceOrganizations method, of class Util.
     */
    @Ignore
    @Test
    public void testGetDataserviceInstanceOrganizations() {
        System.out.println("getDataserviceInstanceOrganizations");
        int dataserviceInstanceId = 0;
        int[] expResult = null;
       //nt[] result = Util.getDataserviceInstanceOrganizations(dataserviceInstanceId);
       // assertArrayEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDatasourceTypes method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDatasourceTypes() {
        System.out.println("getDatasourceTypes");
        List<DatasourceType> expResult = null;
        List<DatasourceType> result = Util.getDatasourceTypes();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDataobjectTypes method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDataobjectTypes() {
        System.out.println("getDataobjectTypes");
        List<DataobjectType> expResult = null;
        List<DataobjectType> result = Util.getDataobjectTypes();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getServiceInstancePropertyConfigMap method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetServiceInstancePropertyConfigMap() {
        System.out.println("getServiceInstancePropertyConfigMap");
        String config = "";
        Map<String, String> expResult = null;
        Map<String, String> result = Util.getServiceInstancePropertyConfigMap(config);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }


    /**
     * Test of getDataobjectTypeMetadataDefinition method, of class Util.
     */
    @Ignore    
    @Test
    public void testGetDataobjectTypeMetadataDefinition() throws Exception {
        System.out.println("getDataobjectTypeMetadataDefinition");
        int dataobjectTypeId = 0;
        boolean includeParentType = false;
        List<Map<String, Object>> expResult = null;
      //  List<Map<String, Object>> result = Util.getDataobjectTypeMetadataDefinition(dataobjectTypeId, includeParentType);
      //  assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDataobjectTypeMetadataDefinitionFromXml method, of class Util.
     */
    @Ignore
    @Test
    public void testGetDataobjectTypeMetadataDefinitionFromXml() throws Exception {
        System.out.println("getDataobjectTypeMetadataDefinitionFromXml");
        List<Map<String, Object>> metadtaDefintion = null;
        String metadataXml = "";
        //Util.getDataobjectTypeMetadataDefinitionFromXml(metadtaDefintion, metadataXml);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}
