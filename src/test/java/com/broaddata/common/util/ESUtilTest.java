/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.thrift.dataservice.SearchResponse;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.ESUtil;
import com.broaddata.common.util.RuntimeContext;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.JSONObject;
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
public class ESUtilTest {
    
    public ESUtilTest() {
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
     * Test of getClient method, of class ESUtil.
     */

     @Ignore
    @Test
    public void testGetDocument() throws Exception
    {
    
        int organizationId = 100;
        
        Util.commonServiceIPs="127.0.0.1";
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
        
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
              
        Client esClient = ESUtil.getClient(em, platformEm, 1, false);
 
        String dataobjectId = "081E9AB4157A5F628E93436DB994A1F1EADCF3EA";
       
        JSONObject obj = ESUtil.getDocument(esClient,organizationId, dataobjectId, null);
        
        int k;
    }
    
    @Ignore
    @Test
    public void testSnapshot()
    {
        Util.commonServiceIPs="127.0.0.1";
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
        
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
              
        Client esClient = ESUtil.getClient(em, platformEm, 1, false);
        
        String snapshotRepositoryName = "edf_backup";
        String snapshotLocation = "D:\\esbak";
        
        if ( !ESUtil.ifSnapshortRepositoryExist(esClient,snapshotRepositoryName) )
            ESUtil.createSnapshotRepository(esClient,snapshotRepositoryName,snapshotLocation);
        
        boolean waitForComplete = false;
        String snapshotPrefix = "edf";
        String[] indexPatternToSnapshot = new String[]{"datastore*","datamart*"};
        ESUtil.createSnapshot(esClient,snapshotRepositoryName,snapshotPrefix,indexPatternToSnapshot,waitForComplete);
   
        ESUtil.deleteSnapshot(esClient,snapshotRepositoryName,"edf-2017-06-08153523");
        
        ESUtil.deleteSnapshotRepository(esClient, snapshotRepositoryName);
        int i=0;
    }
    
    @Ignore
    @Test
    public void test1() throws Exception
    {
        
        Util.commonServiceIPs="127.0.0.1";
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
        
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        
        DatasourceConnection ds = em.find(DatasourceConnection.class, 11);
        
        Connection conn = JDBCUtil.getJdbcConnection(ds);
        
        Client esClient = ESUtil.getClient(em, platformEm, 1, false);
        
        String[] indexTypes = new String[]{"CORE_BDFMHQAC_type"};
        String[] selectedFields = new String[]{};
        String[] selectedSortFields = new String[]{"checksum,ASC"};
        
        Map<String, Object> searchRet;searchRet = ESUtil.retrieveAllDatasetWithScroll(250,600,esClient, new String[]{}, indexTypes, "", "", selectedFields, selectedSortFields,null);
            
        List<Map<String,Object>> searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");
                  
        int currentHits = (Integer)searchRet.get("currentHits");
 
        for(Map<String,Object> result:searchResults)
        {
            String id = (String)result.get("id");
            System.out.println(" id="+id);
        }    
    }
    
    @Ignore
    @Test
    public void testGetClient() {
        System.out.println("getClient");

        Util.commonServiceIPs="127.0.0.1";
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
        
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        
        Client esClient = ESUtil.getClient(em, platformEm, 1, false);
        
        String[] indexTypes = new String[]{"CORE_BDFMHQAC_type"};
        String[] selectedFields = new String[]{};
        
        Map<String,Object> ret = ESUtil.retrieveAllDataset(esClient, new String[]{}, indexTypes, "", "", selectedFields, null, 50000,null);
        
        
      //  JSONObject jsonObject =  ESUtil.getDocument(em,platformEm,100,"30CBCA475D58057F9071B38DACCAC5AE997BCBF8",1,1,1);
  
 
    }
 @Ignore
    @Test
    public void testAddIndexToAlias() 
    {
        System.out.println("AddIndexToAlias");
        Util.commonServiceIPs="127.0.0.1";
        
        EntityManager em=Util.getEntityManagerFactory(100).createEntityManager();
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        
        Client client = ESUtil.getClient(em,platformEm,1,false);
       
        String indexName1 = "test11111111111";
        String indexName2 = "test22222222";
        String aliasName = "myalias";
        
        try 
        {
            ESUtil.createIndexWithoutType(client, indexName1);
            ESUtil.createIndexWithoutType(client, indexName2);
            
            ESUtil.addIndexToAlias(client, aliasName, indexName1, "add");
            ESUtil.addIndexToAlias(client, aliasName, indexName2, "add");
            
            ESUtil.addIndexToAlias(client, aliasName, indexName1, "remove");
            
        }
        catch(Exception e)
        {
            
        }   

    }
    
    /**
     * Test of createIndex method, of class ESUtil.
     */    

    @Ignore
    @Test
    public void testCreateIndex() {
        System.out.println("createIndex");
        Util.commonServiceIPs="127.0.0.1";
        
        EntityManager em=Util.getEntityManagerFactory(100).createEntityManager();
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        
        Client client = ESUtil.getClient(em,platformEm,1,false);
        
        RuntimeContext.dataobjectTypes = Util.getDataobjectTypes();
        String indexName = "test000000";
        try {
            ESUtil.createIndex(platformEm, client, indexName, true);
        }
        catch(Exception e)
        {
            
        }
        

    }

    /**
     * Test of putMappingToIndex method, of class ESUtil.
     */
  @Ignore  
    @Test
    public void testPutMappingToIndex() {
        System.out.println("putMappingToIndex");
 
        Util.commonServiceIPs="127.0.0.1";
        
        EntityManager em=Util.getEntityManagerFactory(100).createEntityManager();
        EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
        
        Client client = ESUtil.getClient(em,platformEm,1,false);
 
        String indexName = "datastore_100_1_data_event_yearmonth_000000_1";
        String indexType = "AAA666_type";
       
        try
        {   
            ESUtil.createIndexWithoutType(client, indexName);
             
            DataobjectType dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, indexType);

            XContentBuilder typeMapping = ESUtil.createTypeMapping(platformEm, dataobjectType, true);
            
            ESUtil.putMappingToIndex(client, indexName, indexType, typeMapping);
        }
        catch(Exception e)
        {
            System.out.print(" failed! e="+e);
        }
    }

    /**
     * Test of IndexDocument method, of class ESUtil.
     */
    @Ignore    
    @Test
    public void testIndexDocument() {
        System.out.println("IndexDocument");
        Client client = null;
        String indexName = "";
        String indexTypeName = "";
        String docId = "";
        Map<String, Object> jsonMap = null;
        long expResult = 0L;
        //long result = ESUtil.indexDocument(client, indexName, indexTypeName, docId, jsonMap);
        //assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of searchDocument method, of class ESUtil.
     */
        @Ignore
    @Test
    public void testSearchDocument() {
        System.out.println("searchDocument");
        Client client = null;
        String indexName = "";
        String queryStr = "";
        String filterStr = "";
        int maxExpectedHits = 0;
        List expResult = null;
      //  List result = ESUtil.searchDocument(client, indexName, queryStr, filterStr, maxExpectedHits);
     //   assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}
