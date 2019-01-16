/*
 * ESUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.json.JSONObject;
import java.net.InetAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.math.BigDecimal;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.MetadataIndexType;
import com.broaddata.common.model.enumeration.RelativeTimeType;
import com.broaddata.common.model.enumeration.TimeUnit;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.IndexSet;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.ServiceInstance;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

public class ESUtil
{    
    static final Logger log=Logger.getLogger("ESUtil");       
    private static final Map<Integer,Client> searchEngineClientList = new HashMap<>();
    private static final Lock lock = new ReentrantLock();
    private static final Lock sendToESlock = new ReentrantLock();
    private static final int MAX_ID_NUMBER_PER_TIME = 900; // es query limit
            
    public static long persistRecord(Client esClient,String indexName,String indexTypeName,String recordId,Map<String, Object> recordJsonMap,long version) throws Exception
    { 
        int retry = CommonKeys.ELASTICSEARCH_RETRY;
        IndexResponse response = null;

        while(retry >0 )
        {
            retry--;
                    
            try
            {
                IndicesExistsResponse ieRep = esClient.admin().indices().prepareExists(indexName).execute().actionGet();

                if ( ieRep.isExists() == false )
                    ESUtil.createIndexWithoutType(esClient, indexName);

                if ( version > 0 )
                    response = esClient.prepareIndex(indexName, indexTypeName)
                        .setId(recordId)
                        .setSource(recordJsonMap)
                        .setVersion(version)
                        .execute()
                        .actionGet();
                else
                    response = esClient.prepareIndex(indexName, indexTypeName)
                        .setId(recordId)
                        .setSource(recordJsonMap)
                        .execute()
                        .actionGet();
                
                return response.getVersion();
            }
            catch(Exception e)
            {
                log.error("persistRecord() failed!  recordId="+recordId+" error="+e);
                //Tool.SleepAWhile(1, 0);
                
                if ( e instanceof TypeMissingException || e instanceof StrictDynamicMappingException || retry==0 )
                    throw e;
            }
        }
        
        return 0;
    }
    
    public static long updateRecord(Client esClient,String indexName,String indexTypeName,String recordId,Map<String, Object> recordJsonMap,long version) throws Exception
    { 
        int retry = CommonKeys.ELASTICSEARCH_RETRY;
        UpdateResponse response = null;

        while(retry >0 )
            retry--;
        {
                    
            try
            {
                IndicesExistsResponse ieRep = esClient.admin().indices().prepareExists(indexName).execute().actionGet();

                if ( ieRep.isExists() == false )
                    ESUtil.createIndexWithoutType(esClient, indexName);

                if ( version > 0 )
                {
                     response = esClient.prepareUpdate().setIndex(indexName).setType(indexTypeName)
                        .setId(recordId)
                        .setDoc(recordJsonMap)
                        .setVersion(version)
                        .get();
                }
                else
                {
                    response = esClient.prepareUpdate().setIndex(indexName).setType(indexTypeName)
                        .setId(recordId)
                        .setDoc(recordJsonMap)
                        .get();
                }
                
                return response.getVersion();
            }
            catch(Exception e)
            {
                log.error("persistRecord() failed!  recordId="+recordId+" error="+e);
                //Tool.SleepAWhile(1, 0);
                
                if ( e instanceof TypeMissingException || e instanceof StrictDynamicMappingException || retry==0 )
                    throw e;
            }
        }
        
        return 0;
    }
    
     public static long persistRecordWithVersion(Client esClient,String indexName,String indexTypeName,String recordId,Map<String, Object> recordJsonMap,long version) throws Exception
    { 
        int retry = CommonKeys.ELASTICSEARCH_RETRY;
        IndexResponse response = null;

        while(retry >0 )
        {
            retry--;
                    
            try
            {
                IndicesExistsResponse ieRep = esClient.admin().indices().prepareExists(indexName).execute().actionGet();

                if ( ieRep.isExists() == false )
                    ESUtil.createIndexWithoutType(esClient, indexName);
 
                response = esClient.prepareIndex(indexName, indexTypeName)
                    .setId(recordId)
                    .setSource(recordJsonMap)
                    .setVersion(version)
                    .execute()
                    .actionGet();
                                 
                return response.getVersion();
            }
            catch(Exception e)
            {
                log.error("persistRecord() failed!  recordId="+recordId+" error="+e);
                //Tool.SleepAWhile(1, 0);
                
                if ( e instanceof TypeMissingException || e instanceof StrictDynamicMappingException || retry==0 )
                    throw e;
            }
        }
        
        return 0;
    }
      
    public static JSONObject findRecord(Client esClient,String indexName,String indexTypeName,String recordId) throws Exception
    { 
        JSONObject jsonObject = null;
        GetResponse response;

        try
        {
             response = esClient.prepareGet(indexName, indexTypeName, recordId)
                    .setOperationThreaded(false)
                    .execute()
                    .actionGet();

            if ( !response.isExists() )
                return null;

            String jsonStr = response.getSourceAsString();

            if ( jsonStr == null )
            {
                Map<String,GetField> fields = response.getFields();

                if ( fields != null )
                    jsonStr = getResultInJsonForGetField(fields);
            }

            if ( jsonStr == null || jsonStr.trim().isEmpty() )
                return null;

            jsonObject = new JSONObject(jsonStr);
        }
        catch(Exception e)
        {
            //log.error("findRecord() failed!  recordId="+recordId+" e="+e); 
            throw e;
        }
        
        return jsonObject;
    }
    
    public static Map<String,Object> findRecordWithVersion(Client esClient,String indexName,String indexTypeName,String recordId) throws Exception
    { 
        Map<String,Object> ret = new HashMap<>();
        JSONObject jsonObject = null;
        GetResponse response;

        try
        {
             response = esClient.prepareGet(indexName, indexTypeName, recordId)
                    .setOperationThreaded(false)
                    .execute()
                    .actionGet();

            if ( !response.isExists() )
                return null;

            String jsonStr = response.getSourceAsString();

            if ( jsonStr == null )
            {
                Map<String,GetField> fields = response.getFields();

                if ( fields != null )
                    jsonStr = getResultInJsonForGetField(fields);
            }

            if ( jsonStr == null || jsonStr.trim().isEmpty() )
                return null;

            jsonObject = new JSONObject(jsonStr);
        }
        catch(Exception e)
        {
            //log.error("findRecord() failed!  recordId="+recordId+" e="+e);
            throw e;
        }
        
        ret.put("version", response.getVersion());
        ret.put("jsonObject",jsonObject);
        
        return ret;
    }
   
    public static Map<String, Object> getRecord(Client esClient,String indexName,String indexTypeName,String recordId) throws Exception
    { 
        Map<String, Object> recordJsonMap = null;
        GetResponse response;

        try
        {
             response = esClient.prepareGet(indexName, indexTypeName, recordId)
                    .setOperationThreaded(false)
                    .execute()
                    .actionGet();

            if ( !response.isExists() )
                return null;

           recordJsonMap = response.getSourceAsMap();
        }
        catch(Exception e)
        {
            log.error("getRecord() failed!  recordId="+recordId);
            throw e;
        }
        
        return recordJsonMap;
    }
    
    public static BulkResponse sendBulkRequest(BulkRequestBuilder bulkRequest)
    {
        try
        {
            //sendToESlock.lock();// 取得锁 
            return bulkRequest.get(); 
        }
        catch(Exception e)
        {
            log.error("sendBulkRequest() failed! e="+e);
            throw e;
        }
        finally
        {
            //sendToESlock.unlock();
        }
    }
    
    public static boolean checkDocumentExistence(int organizationId,String dataobjectId,String indexName, int repositoryId)
    {
        try
        {                
            Client client = getClient(organizationId,repositoryId,false);

            GetResponse response = client.prepareGet(indexName, null, dataobjectId)
                    .setOperationThreaded(false)
                    .setFetchSource(false)
                    .execute()
                    .actionGet();

            return response.isExists();
        }
        catch(Exception e)
        {
            log.error("checkDocumentExistence() failed! e="+e+" dataobjectId="+dataobjectId);                
            throw e;
        }  
    }
    
    public static Client getClient(EntityManager em, EntityManager platformEm, int repositoryId, boolean needToReConnect)
    {
        Client client = null;
        
        try
        {
            client = searchEngineClientList.get(repositoryId);
            
            if ( needToReConnect && client!=null )
            {
                searchEngineClientList.remove(repositoryId);

                client.close();
                client = null;
            }
            
            if ( client == null )
            {
                lock.lock();// 取得锁
                
                client = searchEngineClientList.get(repositoryId);
                
                if ( client == null )
                {
                    try
                    {
                        if ( platformEm == null || !platformEm.isOpen() )
                            platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
                        
                        boolean sniff = true;
                        String disableSniff = Util.getSystemConfigValue(platformEm,0,"elasticsearch","disable_sniff");
                        
                        if ( disableSniff.equals("yes") )
                            sniff = false;
                        
                        ServiceInstance serviceInstance = Util.getRepositorySearchEngineServiceInstance(em, platformEm, repositoryId);
                        Map<String,String> propertyMap = Util.getConfigPropertyMap(serviceInstance.getConfig());

                        String clusterDiscoveryType = propertyMap.get("clusterDiscoveryType");
                        String clusterName = propertyMap.get("clusterName");

                   /*     if ( clusterDiscoveryType.equals("multicast") )
                        { 
                            org.elasticsearch.node.Node node = NodeBuilder.nodeBuilder().clusterName(clusterName).client(true).data(false).node(); 

                            //启动结点,加入到指定集群, one per cluster
                            node.start();
                            client = node.client();
                        }
                        else // unicast
                        {*/
                            String[] masterNodeIPs = propertyMap.get("masterNodeIPs").trim().split("\\,");
                            //masterNodeIPs = Tool.shuffleArray(masterNodeIPs);
                           
                            /*Settings settings = ImmutableSettings.settingsBuilder()
                                     .put("cluster.name",clusterName)
                                     .put("client.transport.sniff", true)
                                     .build();*/
                            Settings settings = Settings.builder()
                                .put("cluster.name", clusterName)
                                .put("client.transport.sniff", sniff)
                                .build();
                                //.put("client.transport.sniff", true)
                                //.build();
                            
                            //TransportClient tClient = TransportClient.builder().settings(settings).build();
                            
                            log.info(" start to get es client ! clusterName="+clusterName+", masterNodeIPs="+propertyMap.get("masterNodeIPs"));
                            
                            TransportClient tClient = new PreBuiltTransportClient(settings);

                            for(String nodeIP : masterNodeIPs)
                                tClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeIP), 9300));

                            client = tClient;
                       // }

                        searchEngineClientList.put(repositoryId, client);
                    }
                    catch(Throwable e)
                    {
                        log.error(" get es client error! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                    }
                    finally
                    {
                        lock.unlock(); 
                    }
                }
                else
                    lock.unlock();
            }
        }
        catch(Exception e)
        {
            log.error("getClient() failed! repositoryId="+repositoryId+" e="+e);
            throw e;
        }
        
        //log.info("getClient() successful! client="+client);

        return client;
    }
        
    public static Client getClient(int organizationId, int repositoryId, boolean needToReConnect)
    {
        EntityManager em = null;
        EntityManager platformEm = null;
        Client client = null;
        
        try
        {
            client = searchEngineClientList.get(repositoryId);
            
            if ( needToReConnect && client!=null )
            {
                searchEngineClientList.remove(repositoryId);

                client.close();
                client = null;
            }
            
            if ( client == null )
            {
                lock.lock();// 取得锁
                
                client = searchEngineClientList.get(repositoryId);
                
                if ( client == null )
                {
                    try
                    {
                        em = Util.getEntityManagerFactory(organizationId).createEntityManager();
                        platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();

                        boolean sniff = true;
                        String disableSniff = Util.getSystemConfigValue(platformEm,0,"elasticsearch","disable_sniff");
                        
                        if ( disableSniff.equals("yes") )
                            sniff = false;
                        
                        ServiceInstance serviceInstance = Util.getRepositorySearchEngineServiceInstance(em, platformEm, repositoryId);

                        Map<String,String> propertyMap = Util.getConfigPropertyMap(serviceInstance.getConfig());

                        String clusterDiscoveryType = propertyMap.get("clusterDiscoveryType");
                        String clusterName = propertyMap.get("clusterName");

                      /*  if ( clusterDiscoveryType.equals("multicast") )
                        { 
                            org.elasticsearch.node.Node node = NodeBuilder.nodeBuilder().clusterName(clusterName).client(true).data(false).node(); 

                            //启动结点,加入到指定集群, one per cluster
                            node.start();
                            client = node.client();
                        }
                        else // unicast
                        {*/
                            String[] masterNodeIPs = propertyMap.get("masterNodeIPs").trim().split("\\,");
 
                          /*  Settings settings = Settings.settingsBuilder()
                                .put("cluster.name", clusterName)
                                .put("client.transport.sniff", true)
                                .build();*/
                             
                            Settings settings = Settings.builder()
                                .put("cluster.name", clusterName)
                                .put("client.transport.sniff", sniff)
                                .build();
                                        
                            //TransportClient tClient = TransportClient.builder().settings(settings).build();

                            log.info(" start to get es client ! clusterName="+clusterName+", masterNodeIPs="+propertyMap.get("masterNodeIPs"));
                            
                            TransportClient tClient = new PreBuiltTransportClient(settings);

                            for(String nodeIP : masterNodeIPs)
                                tClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeIP), 9300));
 
                            client = tClient;
                       // }

                        searchEngineClientList.put(repositoryId, client);
                    }
                    catch(Exception e)
                    {
                        log.error(" get es client error! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                    }
                    finally
                    {
                        lock.unlock(); 
    
                        if (em!=null)
                            if ( em.isOpen())
                              em.close();

                        if (platformEm!=null)
                            if(platformEm.isOpen())
                                platformEm.close();
                    }
                }
                else
                    lock.unlock();
            }
        }
        catch(Exception e)
        {
            log.error("getClient() failed! repositoryId="+repositoryId+" e="+e);
            throw e;
        }

        return client;
    }
             
    public static Client getClient(EntityManager platformEm,int serviceInstanceId) throws Exception
    {
        Client client = null;
        
        try
        {
             boolean sniff = true;
            String disableSniff = Util.getSystemConfigValue(platformEm,0,"elasticsearch","disable_sniff");

            if ( disableSniff.equals("yes") )
                sniff = false;
                        
            ServiceInstance serviceInstance = platformEm.find(ServiceInstance.class, serviceInstanceId);
        
            Map<String,String> propertyMap = Util.getConfigPropertyMap(serviceInstance.getConfig());
 
            String clusterName = propertyMap.get("clusterName");
            String[] masterNodeIPs = propertyMap.get("masterNodeIPs").trim().split("\\,");
 
            Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", sniff)
                .build();
  
            log.info(" start to get es client ! clusterName="+clusterName+", masterNodeIPs="+propertyMap.get("masterNodeIPs"));
                            
            TransportClient tClient = new PreBuiltTransportClient(settings);

            //TransportClient tClient = TransportClient.builder().settings(settings).build();

            for(String nodeIP : masterNodeIPs)
                tClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeIP), 9300));

            client = tClient;
        }
        catch(Exception e)
        {
            log.error("1111 getClient() failed!  e="+e);
            throw e;
        }

        return client;
    }
          
    public static JSONObject getDocument(Client esClient,int organizationId, String dataobjectId, String indexType)
    {
        String queryStr = String.format("_id:%s",dataobjectId);
            
        String[] indexTypes;
        
        if ( indexType == null || indexType.trim().isEmpty() )
            indexTypes = new String[]{};
        else
            indexTypes = new String[]{indexType};
        
        Map<String,Object> ret = ESUtil.searchDocument(esClient, new String[]{}, indexTypes, new String[]{}, queryStr, null, 1,0, new String[]{}, new String[]{});

        long totalHit = (long) ret.get("totalHits");
        
        if ( totalHit != 1 ) 
            return null;
            
        List<Map<String,Object>> searchResults = (List<Map<String,Object>>)ret.get("searchResults");
    
        if ( searchResults == null || searchResults.isEmpty() )
            return null;
        
        String jsonStr = (String)searchResults.get(0).get("fields");
       
        return new JSONObject(jsonStr);
    }
    
    public static JSONObject getDocument(EntityManager em, EntityManager platformEm, int organizationId, String dataobjectId, int dataobjectVersion,int dataobjectTypeId,int repositoryId) throws Exception
    {
        Client esClient;
        List<IndexSet> indexSetList;
        GetResponse response;
        String jsonStr = null;
        List<String> fieldNames;
        String[] fieldNameArray = null;
        
        int retry = 0;
        boolean needToReConnect = false;
        
        while (retry < CommonKeys.ELASTICSEARCH_RETRY)
        {
            retry++;
            
            try
            {
                //String indexTypeName = DataobjectHelper.getDataobjectIndexTypeName(platformEm, dataobjectTypeId);
                //indexSetList = DataobjectHelper.getDataobjectIndexSetList(em, repositoryId);
                                
                esClient = getClient(em,platformEm,repositoryId,needToReConnect);
 
                return getDocument(esClient,organizationId, dataobjectId, null);
            }
            catch(Exception e)
            {
                log.error("getDocument() failed! e="+e+" dataobjectId="+dataobjectId);                
                throw e;
            }            
        }
                        
        return null;        
    }

     public static JSONObject getDocumentWithFullContents(EntityManager em, EntityManager platformEm, int organizationId, String dataobjectId, int dataobjectVersion,int dataobjectTypeId,int repositoryId) throws Exception
    {
        Client client;
        List<IndexSet> indexSetList;
        GetResponse response;
        String jsonStr = null;
        List<String> fieldNames;
        String[] fieldNameArray = null;
        
        int retry = 0;
        boolean needToReConnect = false;
        
        while (retry < CommonKeys.ELASTICSEARCH_RETRY)
        {
            retry++;
            
            try
            {
                String indexTypeName = DataobjectHelper.getDataobjectIndexTypeName(platformEm, dataobjectTypeId);
                indexSetList = DataobjectHelper.getDataobjectIndexSetList(em, repositoryId);
                
                client = getClient(em,platformEm,repositoryId,needToReConnect);

                for(IndexSet indexSet : indexSetList )
                {        
                    try
                    {
                        jsonStr = getDocumentFromIndexSet1(client, indexSet, indexTypeName, dataobjectId, dataobjectVersion, fieldNameArray);
                        
                        if ( jsonStr != null )
                            break;
                    }
                    catch(Exception e)
                    {
                        log.error("client.prepareGet() failed! e="+e+" indexNameList ="+indexSet.getName()+" dataobjectId="+dataobjectId);
                    }
                }

                //if ( jsonStr == null )
                //    return null;
                if ( jsonStr == null )
                    throw new Exception("dataobject id not found! dataobjectId="+dataobjectId+" version="+dataobjectVersion);
                
                JSONObject jsonObject = new JSONObject(jsonStr);

                return jsonObject; 
            }
            catch(Exception e)
            {
                log.error("getDocument() failed! e="+e+" dataobjectId="+dataobjectId);                
                throw e;
            }            
        }
                        
        return null;        
    }

    public static String getDocumentFromIndexSet1(Client client, IndexSet indexSet, String indexTypeName, String dataobjectId, int dataobjectVersion,String[] fieldNameArray) throws ElasticsearchException 
    {
        GetResponse response;
        String jsonStr;
        String indexName;        
      
        for(int i=indexSet.getCurrentIndexNumber();i>=1;i--)
        {
            indexName = String.format("%s_%d", indexSet.getName().substring(0,indexSet.getName().lastIndexOf("_")), i);
            //log.info(" check indexname="+indexName+" indexType="+indexTypeName+" dataobjectId="+dataobjectId);
                   
            try
            {   
                if ( dataobjectVersion >1 )
                {
                    response = client.prepareGet(indexName, indexTypeName, dataobjectId)
                            .setOperationThreaded(false).setVersion(dataobjectVersion)
                            //.setFetchSource(false)
                            //.setFields(fieldNameArray)
                            .execute()
                            .actionGet();
                }
                else
                {
                    response = client.prepareGet(indexName, indexTypeName, dataobjectId)
                            .setOperationThreaded(false)
                            //.setFields(fieldNameArray)
                            //.setFetchSource(false)
                            .execute()
                            .actionGet();
                }
            }
            catch(Exception e)
            {
                log.error(" prepare get failed! e="+e);
                continue;
            }
           
            if ( !response.isExists() )
                continue;
            
            //log.info(" dataobject exists! jsonStr="+response.getSourceAsString());
            
            log.info(" dataobject exists! dataobject id="+dataobjectId);
            
            indexName = response.getIndex();
            
            jsonStr = response.getSourceAsString();

            if ( jsonStr == null )
            {
                Map<String,GetField> fields = response.getFields();

                if ( fields != null )
                    jsonStr = getResultInJsonForGetField(fields);
            }

            if ( jsonStr != null )
            {
                jsonStr = jsonStr.substring(0,jsonStr.length()-1);
                
                jsonStr = String.format("%s,\"index_name\":\"%s\"}",jsonStr,indexName);
                return jsonStr;
            }
        }
        
        return null;
    }
    
    public static String getTargetDocumentIndexName(Client client, IndexSet indexSet, String indexTypeName, String dataobjectId) throws ElasticsearchException, Exception 
    {
        String indexName;
        String targetIndexName;
  
        if ( indexSet.getCurrentIndexNumber() == 1 )
        {
            indexName = String.format("%s_%d", indexSet.getName().substring(0,indexSet.getName().lastIndexOf("_")), indexSet.getCurrentIndexNumber());
            return indexName;
        }
      
        targetIndexName = getDocumentIndex(client, new String[]{indexSet.getName()}, new String[]{indexTypeName}, dataobjectId);
        
        if ( targetIndexName != null && targetIndexName.startsWith("error:") )
        {
            // delete document 
            
            indexName = targetIndexName.substring("error:".length());
            String[] indexs = indexName.split("\\;");
            
            for(String index : indexs )
            {
                log.info(" delete document="+dataobjectId+" in index+"+index);
                
                DeleteResponse res = client.prepareDelete(index, indexTypeName, dataobjectId)
                        .execute()
                        .actionGet();
            }
            
            targetIndexName = null;
        }
           
        if ( targetIndexName == null )
           targetIndexName = String.format("%s_%d", indexSet.getName().substring(0,indexSet.getName().lastIndexOf("_")), indexSet.getCurrentIndexNumber());
     
        return targetIndexName;
    }
        
    public static String getTargetDocumentIndexName1(Client client, IndexSet indexSet, String indexTypeName, String dataobjectId) throws ElasticsearchException, Exception 
    {
        String indexName;
        String targetIndexName;
              
        targetIndexName = getDocumentIndex(client, new String[]{indexSet.getName()}, new String[]{indexTypeName}, dataobjectId);
        
        if ( targetIndexName != null && targetIndexName.startsWith("error:") )
        {
            // delete document 
            
            indexName = targetIndexName.substring("error:".length());
            String[] indexs = indexName.split("\\;");
            
            for(String index : indexs )
            {
                log.info("5555555555555555 delete document="+dataobjectId+" in index+"+index);
                
                DeleteResponse res = client.prepareDelete(index, indexTypeName, dataobjectId)
                        .execute()
                        .actionGet();
            }
            
            targetIndexName = null;
        }
           
        if ( targetIndexName == null )
           targetIndexName = String.format("NEW:%s_%d", indexSet.getName().substring(0,indexSet.getName().lastIndexOf("_")), indexSet.getCurrentIndexNumber());
     
        return targetIndexName;
    }
    
    public static  Map<String,String> getTargetDocumentIndexNames(Client client, IndexSet indexSet, String indexTypeName, List<String> documentIdList) throws ElasticsearchException, Exception 
    {
        String currentIndexName;
        Map<String,String> documentIndexMap = new HashMap<>();
           
        currentIndexName = String.format("%s_%d", indexSet.getName().substring(0,indexSet.getName().lastIndexOf("_")), indexSet.getCurrentIndexNumber());
          
        if ( indexSet.getCurrentIndexNumber() == 1 )
        {   
            for(String documentId : documentIdList )
                documentIndexMap.put(documentId,currentIndexName);
            
            return documentIndexMap;
        }
        
        int retry = 0;
        
        while(true)
        {
            retry ++;
            
            try
            {
                int k = documentIdList.size() / MAX_ID_NUMBER_PER_TIME +1;
                
                for(int i=0;i<k;i++)
                    getDocumentIndexs(client, new String[]{indexSet.getName()}, new String[]{indexTypeName}, documentIdList,i*MAX_ID_NUMBER_PER_TIME,MAX_ID_NUMBER_PER_TIME,documentIndexMap);
                
                break;
            }
            catch(Exception e)
            {
                log.error(" getDocumentIndexs() failed! e="+e+" ");
                        
                if ( retry > 5 )
                    throw e;
                
                Tool.SleepAWhile(1, 0);
            }
        }
      
        for(String documentId : documentIdList )
        {
            String indexName = documentIndexMap.get(documentId);
            
            if ( indexName == null )
                documentIndexMap.put(documentId, currentIndexName);
        }
 
        return documentIndexMap;
    }
    
    /*
     public static String getTargetDocumentIndexName(Client client, IndexSet indexSet, String indexTypeName, String dataobjectId, int dataobjectVersion) throws ElasticsearchException 
    {
        GetResponse response;
        String indexName;
     
        for(int i=indexSet.getCurrentIndexNumber()-1;i>=1;i--)  // skip current index
        {
            indexName = String.format("%s_%d", indexSet.getName().substring(0,indexSet.getName().lastIndexOf("_")), i);
            
            log.info("check dataobjectId="+dataobjectId+"  in previous index ="+indexName);
            
            if ( dataobjectVersion >0 )
            {
                response = client.prepareGet(indexName, indexTypeName, dataobjectId)
                        .setOperationThreaded(false).setVersion(dataobjectVersion)
                        .setFetchSource(false)
                        .execute()
                        .actionGet();
            }
            else
            {
                response = client.prepareGet(indexName, indexTypeName, dataobjectId)
                        .setOperationThreaded(false)
                        .setFetchSource(false)
                        .execute()
                        .actionGet();
            }

            if ( response.isExists() )
            {
                log.info(" found dataobjectId="+dataobjectId+"  in previous index ="+indexName);
                return indexName;
            }
        }
                        
        indexName = String.format("%s_%d", indexSet.getName().substring(0,indexSet.getName().lastIndexOf("_")), indexSet.getCurrentIndexNumber());
        //log.info("put dataobjectId="+dataobjectId+"  in latest index ="+indexName);
               
        return indexName;
    }
        */
    public static long addOrupdateDocument(EntityManager em, EntityManager platformEm, int organizationId, String dataobjectId, int dataobjectVersion, JSONObject jsonObject,int dataobjectTypeId,int repositoryId)
    {         
        Client client = null;
        
        int retry = 0;
        boolean needToReConnect = false;
        
        while(retry < CommonKeys.ELASTICSEARCH_RETRY)
        {
            retry++;
                    
            try
            {
                String indexTypeName = DataobjectHelper.getDataobjectIndexTypeName(platformEm, dataobjectTypeId);
                List<String> indexNameList = DataobjectHelper.getDataobjectIndexNameList(em, repositoryId);
                String documentId = String.format("%s",dataobjectId);
                //String documentId = dataobject.getId()+String.valueOf(dataobjectVersion);

                client = getClient(em,platformEm,repositoryId,needToReConnect);

                for(String indexName : indexNameList) // need to change
                {
                    IndexResponse response = client.prepareIndex(indexName,indexTypeName)
                        .setId(documentId)
                        .setSource(jsonObject.toString())
                        .execute()
                        .actionGet();
                }

                return 0;
            }
            catch(Exception e)
            {
                log.error("addOrupdateDocument() failed!  dataobjectId="+dataobjectId+" stacktrace="+ExceptionUtils.getStackTrace(e));
                needToReConnect = true;
                
                if ( retry == CommonKeys.ELASTICSEARCH_RETRY )
                    throw e;
            }
        }
                
        return -1;
    }
    
     public static long deleteDocument(EntityManager em, EntityManager platformEm, int organizationId, String dataobjectId,int dataobjectVersion,int dataobjectTypeId,int repositoryId)
    {       
        Client client;
        
        int retry = 0;
        boolean needToReConnect = false;
        
        while (retry < CommonKeys.ELASTICSEARCH_RETRY)
        {
            retry++;        
            try 
            {
                Dataobject dataobject =  DataobjectHelper.getDataobject(em,dataobjectId);

                String indexTypeName = DataobjectHelper.getDataobjectIndexTypeName(platformEm, dataobject.getDataobjectType());
                List<String> indexNameList = DataobjectHelper.getDataobjectIndexNameList(em, repositoryId);
                String documentId = dataobject.getId()+String.valueOf(dataobject.getCurrentVersion());

                client = getClient(em,platformEm,dataobject.getRepositoryId(),needToReConnect);

                for(String indexName: indexNameList)
                {
                    DeleteResponse response = client.prepareDelete(indexName, indexTypeName, documentId)
                            .execute()
                            .actionGet();
                }
                
                return 0;
            }
            catch(Exception e)
            {
                log.error("addOrupdateDocument() failed!  dataobjectId="+dataobjectId);
                e.printStackTrace();
                needToReConnect = true;
            }         
        }
        
        return -1;
    }   
           
    public static Map<String,String> retrieveIndexInfo(Client client, String indexName) throws Exception
    {
        Map<String,String> map = new HashMap<>();
                            
        try 
        {   
            IndicesStatsRequest request = new IndicesStatsRequest();
            request.store(true);
            request.docs(true);
            request.indices(indexName);
            request.indexing(true);
            
            IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();
            
            IndexStats status = response.getIndex(indexName); 
            long count = status.getTotal().getDocs().getCount();
            long size = status.getTotal().getStore().getSizeInBytes();
            
            map.put("docCount",String.valueOf(count));
            map.put("docSize",String.valueOf(size));
                        
            return map;
        } 
        catch (Exception ex) 
        {
            log.error("retrieveIndexInfo() failed! e="+ex);
            throw ex;
        }
    }
           
    public static List<Map<String,String>> retrieveIndexTypeInfo(Client client, String indexName) throws Exception
    {                           
        try 
        {   
            return retrieveDatasetAggregation(client, new String[]{indexName}, new String[]{}, "", "", 0, 0,  new String[]{"_type"},new String[]{"count(*)"},new String[]{},null);
        } 
        catch (Exception ex) 
        {
            log.error("retrieveIndexTypeInfo() failed! e="+ex);
            throw ex;
        }
    }    
     
    public static List<Map<String,String>> retrieveIndexTypeInfo(Client client, String indexName, String[] indexTypes) throws Exception
    {                           
        try 
        {   
            return retrieveDatasetAggregation(client, new String[]{indexName}, indexTypes, "", "", 0, 0,  new String[]{"_type"},new String[]{"count(*)"},new String[]{},null);
        } 
        catch (Exception ex) 
        {
            log.error("retrieveIndexTypeInfo() failed! e="+ex);
            throw ex;
        }
    }   
     
    public static synchronized void createIndex(EntityManager platformEm, Client client, String indexName,boolean withDataobjectFields) throws Exception
    {
        try 
        {   
            IndicesExistsResponse ieRep = client.admin().indices().prepareExists(indexName).execute().actionGet();
            if ( ieRep.isExists() )
                return;            
            
            Settings settings = Settings.builder()
                     .put("index.mapper.dynamic", false)
                     .build();
            
            CreateIndexResponse cRep = client.admin().indices().prepareCreate(indexName).setSettings(settings).execute().actionGet();
            cRep.isAcknowledged(); 
            
            List<DataobjectType> dataobjectTypeList = Util.getDataobjectTypes();

            for (DataobjectType type : dataobjectTypeList) 
            {
                XContentBuilder typeMapping = createTypeMapping(platformEm, type, withDataobjectFields);                  
                putMappingToIndex(client, indexName, type.getIndexTypeName(), typeMapping);
                Thread.sleep(100);
            }
        } 
        catch (Exception ex) 
        {
            log.error("createIndex() failed! e="+ex);
            throw ex;
        }
    }

    public static synchronized void addIndexToAlias(Client client, String aliasName, String indexName, String action) throws Exception
    {
        int retry = 10;
        
        while(true)
        {
            retry--;
            
            try 
            {
                log.info("addIndexToAlias()  aliasName="+aliasName+" indexName="+indexName);

                if ( action.equals("add") )
                {
                    client.admin().indices().prepareAliases().addAlias(indexName,aliasName).execute().actionGet();
                }
                else
                if ( action.equals("remove") )
                {
                    client.admin().indices().prepareAliases().removeAlias(indexName,aliasName).execute().actionGet();
                }

                log.info("addIndexToAlias() done");
                break;
            } 
            catch (Exception e)
            {
                log.error(String.format("addIndexToAlias() failed! e=%s, aliasName=%s indexName=%s action=%s",e,aliasName,indexName,action));
               
                if ( retry == 0 )
                    throw e;
                
                Tool.SleepAWhile(1, 0);
            }
        }
    }
    
    public static synchronized void createIndexWithoutType(Client client, String indexName) throws Exception
    {
        int retry = 100;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {   
                IndicesExistsResponse  ieRep = client.admin().indices().prepareExists(indexName).execute().actionGet();
                if ( ieRep.isExists() )
                    return;            

                Settings settings = Settings.builder()
                         .put("index.mapper.dynamic", false)
                         .build();

                CreateIndexResponse cRep = client.admin().indices().prepareCreate(indexName).setSettings(settings) .execute().actionGet();
                cRep.isAcknowledged();
                
                break;
            } 
            catch (Exception e) 
            {
                log.error("createIndex() failed! e="+e);
                
                if ( retry==0 )
                    throw e;
                
                Tool.SleepAWhile(1, 0);
            }
        }
    }

    public static void putMappingToIndex(Client client, String indexName, String indexTypeName, XContentBuilder typeMapping)
    {
        log.info("start to putMappingToIndex indexname="+indexName+" indexTypeName="+indexTypeName+ " mapping="+typeMapping);
        
        try 
        {
            PutMappingRequestBuilder builder = client.admin().indices().preparePutMapping(indexName);
            builder.setType(indexTypeName);
            builder.setSource(typeMapping);
            PutMappingResponse response = builder.execute().actionGet();
            response.isAcknowledged();
        }
        catch(Exception e)
        {
            log.info("putMappingToIndex() failed! indexnam="+indexName+" indexTypeName="+indexTypeName+ " mapping="+typeMapping);
            throw e;
        }
        
        log.info("putMappingToIndex() successful!  indexnam="+indexName+" indexTypeName="+indexTypeName+ " mapping="+typeMapping);
    }
    
    public static boolean isIndexExists(Client client, String indexName)
    {
        try 
        {   
            IndicesExistsResponse ieRep = client.admin().indices().prepareExists(indexName).execute().actionGet();
            return ieRep.isExists();
        } 
        catch (Exception e) 
        {
            log.error("isIndexExists() failed! e="+e);
            throw e;
        }
    }
    
    public static void deleteIndex(Client client, String indexName)
    {
        try
        {   
            DeleteIndexResponse diRep = client.admin().indices().prepareDelete(indexName).execute().actionGet();
            diRep.isAcknowledged();
        } 
        catch (Exception e) 
        {
            log.error("deleteIndex() failed! e="+e+" indexName="+indexName);
            throw e;
        }
    }
    
    public static long indexDocument(Client client, String indexName, String indexTypeName, String docId, Map<String,Object> jsonMap) throws Exception
    {
        Date startTime = new Date();
        
        IndicesExistsResponse ieRep = client.admin().indices().prepareExists(indexName).execute().actionGet();
        
        if ( ieRep.isExists() == false )
        {
            String indexSetName = indexName.substring(0, indexName.lastIndexOf("_"))+"_alias";
            ESUtil.createIndexWithoutType(client, indexName);    

            Tool.SleepAWhile(1, 0); // sleep 3 second

            for(int i=0;i<3;i++)
            {
                ESUtil.addIndexToAlias(client, indexSetName, indexName, "add");
                Tool.SleepAWhile(1, 0); // sleep 3 second
            }
        }
             
        IndexResponse response = client.prepareIndex(indexName, indexTypeName)
                .setId(docId)
                .setSource(jsonMap)
                .execute()
                .actionGet();
        
        log.info(String.format("writing to elasticsearch, took [%d] ms",new Date().getTime()-startTime.getTime()));
        
        return response.getVersion();
    }
        
    public static Map<String,Object> searchDocument(Client client, String[] indexNames, String[] indexTypes, String[] queryFields, String queryStr, String filterStr, int maxExpectedHits, int searchFrom, String[] aggregationFields,String[] selectedSortFields)
    {
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> searchResults = new ArrayList<>();
        Map<String,Object> searchResult;
        //QueryStringQueryBuilder query = null;
        //FilterBuilder filter = null;
        TermsAggregationBuilder termsBuilder;
        SortBuilder sBuilder;
        QueryStringQueryBuilder qBuilder;

        //log.info(String.format("searchDocument: index names=(%s) index types = (%s) fields = (%s) queryStr=(%s) filterStr=(%s) ",indexNames,indexTypes, queryFields, queryStr, filterStr));
        
        //for(String indexName:indexNames)
        //    log.info(" index name = "+indexName);
        
      /*  for(String indexType:indexTypes)
            log.info(" index type ="+ indexType);
        
        for(String field:queryFields)
            log.debug(" fields = "+field);

        if ( aggregationFields != null )
        for(String aField : aggregationFields)
            log.debug(" aggregation field = "+aField); */
        
        queryStr = processQueryStrForSearchDocument(queryStr);
        
        qBuilder = getQueryBuilder(queryStr,filterStr);
        
        for(String field:queryFields)
            qBuilder.field(field);
    
        // builder
        SearchRequestBuilder builder = client.prepareSearch(indexNames);
        builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value

        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);
        
        builder.setQuery(qBuilder);
        
        builder.setSize(maxExpectedHits);
        builder.setFrom(searchFrom); 
        builder.addStoredField("*"); 

        //builder.addFields("dataobject_type","dataobject_type_name","target_repository_id","organization_id","dataobject_name","object_size","datasource_type","last_stored_time");

       /* builder.addHighlightedField("*");
        builder.setHighlighterRequireFieldMatch(false);
        builder.setHighlighterPreTags("<span style=\"color:red\">");
        builder.setHighlighterPostTags("</span>");*/
              
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        
        builder.highlighter(highlightBuilder);

      /*  if ( aggregationFields != null && aggregationFields.length > 0 )
        {
            for (String aggregationField : aggregationFields) // metadataname,90/ metadataname 80/
            {
                String[] aInfo = aggregationField.split("\\,");  // aInfo[0] - name, aInfo[1] - size, aInfo[2] - ASCEND
                termsBuilder = AggregationBuilders.terms(aInfo[0]+"_terms").field(aInfo[0]).order(Terms.Order.count(aInfo[2].equals("ASCEND"))).size(Integer.parseInt(aInfo[1]));              
                builder.addAggregation(termsBuilder);
            }
        }*/
      
        if ( aggregationFields != null && aggregationFields.length > 0 )
        {
            for (String aggregationField : aggregationFields) // metadataname,90/ metadataname 80/
            {
                String[] aInfo = aggregationField.split("\\,");  // aInfo[0] - name, aInfo[1] - size, aInfo[2] - ASCEND
                int size = Integer.parseInt(aInfo[1]);
                if ( size == 0 )
                    size = 1000;
                
                termsBuilder = AggregationBuilders.terms(aInfo[0]+"_terms").field(aInfo[0]).order(Terms.Order.count(aInfo[2].equals("ASCEND"))).size(size);              
                builder.addAggregation(termsBuilder);
            }
        }
        
        for(String sortField : selectedSortFields)
        {
            String[] str=sortField.split("\\,");
        
            if ( str[1].equals("ASCEND") )
                sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.ASC);
            else
                sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.DESC);
            
            builder.addSort(sBuilder);
        }
          
        builder.setVersion(true); // return version for every search
          
        SearchResponse response = builder.execute().actionGet();           

        SearchHits shs = response.getHits();   
        
        log.info("total hits = "+shs.getTotalHits());

        float scoreForSortingByEventTime = (float)1.0;
        
        for (SearchHit hit : shs) 
        {
            searchResult = new HashMap<>();

            searchResult.put("id", String.format("%s%d",hit.getId(),hit.getVersion())); 
             
            if ( selectedSortFields.length > 0 )  // sort by event time
            {
                scoreForSortingByEventTime = scoreForSortingByEventTime - (float)0.0000001;
                searchResult.put("score", scoreForSortingByEventTime);
            }
            else // sort by relevance
                searchResult.put("score", hit.getScore());
            
            Map<String,SearchHitField> fields = hit.getFields();
            searchResult.put("fields", getResultInJsonForSearchHitField(fields));
            
            Map<String, HighlightField> highlightFields = hit.getHighlightFields(); 
            searchResult.put("highlights", getHighlightsInJson(highlightFields));           
 
            searchResults.add(searchResult);
        }
        
        result.put("searchResults", searchResults);
        result.put("totalHits", shs.getTotalHits());
        result.put("currentHits", searchResults.size());

        Aggregations aggregations = response.getAggregations();
        
        if ( aggregations != null )
            processAggregations(aggregations, result);  
        
        return result;
    }
        
    public static String getDocumentIndex(Client client, String[] indexNames, String[] indexTypes, String documentId) throws Exception
    {
        QueryStringQueryBuilder qBuilder;

        qBuilder = QueryBuilders.queryStringQuery(String.format("_id:%s",documentId));
    
        SearchRequestBuilder builder = client.prepareSearch(indexNames);
        builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value

        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);
        
        builder.setQuery(qBuilder);
  
        builder.addStoredField("dataobject_type"); 
        builder.addStoredField("_id"); 
  
        builder.setVersion(true); // return version for every search
                 
        SearchResponse response = builder.execute().actionGet();           

        SearchHits shs = response.getHits();   
        
        //log.info("getDocumentIndex total hits = "+shs.getTotalHits()+" documentId="+documentId);
 
        if ( shs.getTotalHits() == 0 )
            return null;
        
        if ( shs.getTotalHits() > 1 )
        {
            //throw new Exception("more than one index for this document! size="+shs.getTotalHits());
         
            String indexNameStr = "error:";
            
            for(SearchHit hit : shs.getHits() )
            {
                indexNameStr += hit.getIndex() + ";";
            }
              
            log.error("5555555555555555 found duplicated document indexNamestr="+indexNameStr);
 
            return indexNameStr;
        }
            
        SearchHit hit = shs.getAt(0);
        
        return hit.getIndex();
    }

    public static void getDocumentIndexs(Client client, String[] indexNames, String[] indexTypes, List<String> documentIdList,int startPosition,int len,Map<String,String> documentIndexMap) throws Exception
    {
        QueryStringQueryBuilder qBuilder;
        int len_this_time=0;

        String queryStr = "_id:( ";
        
        //for(String documentId : documentIdList)
        //   queryStr += documentId + " ";
        
        for(int i=startPosition;i<startPosition+len;i++)
        {
            if ( i >= documentIdList.size() )
                break;
            
            queryStr += documentIdList.get(i) + " ";
            len_this_time++;
        }
        
        queryStr += " )";
        
        if ( queryStr.equals("_id:(  )") )
            return;
        
        qBuilder = QueryBuilders.queryStringQuery(queryStr);
    
        SearchRequestBuilder builder = client.prepareSearch(indexNames);
        builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value

        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);
        
        builder.setQuery(qBuilder);
  
        //builder.addStoredField("dataobject_type","_id");
        
        builder.addStoredField("dataobject_type");
        builder.addStoredField("_id");
  
        builder.setVersion(true); // return version for every search
        
        builder.setSize(len_this_time*2);
          
        Date date1 = new Date();
        
        SearchResponse response = builder.execute().actionGet();
        SearchHits shs = response.getHits();
        
        log.info("check index total hits = "+shs.getTotalHits()+", took "+(new Date().getTime()-date1.getTime())+" ms");
 
        if ( shs.getTotalHits() == 0 )
            return;
        
        if ( shs.getTotalHits() > len_this_time )
            throw new Exception(" more than one index for some document");
 
        for(SearchHit hit : shs )
            documentIndexMap.put(hit.getId(), hit.getIndex());
    }

    public static Map<String,Object> retrieveDataset(Client client, String[] indexNames, String[] indexTypes, String queryStr, String filterStr, int maxExpectedHits, int searchFrom, String[] selectedFields, String[] selectedSortFields,String[] aggregationFields,String currentTime)
    {
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> searchResults = new ArrayList<>();
        Map<String,Object> searchResult;
        //QueryStringQueryBuilder query = null;
        //FilterBuilder filter = null;
        SortBuilder sBuilder;
        TermsAggregationBuilder termsBuilder;
        QueryBuilder qBuilder;

        //log.info(String.format("retrieveDataset: index names=(%s) index types = (%s) queryStr=(%s) filterStr=(%s) selectedFields=(%s) selectedSortFields=(%s)",indexNames,indexTypes,queryStr,filterStr,selectedFields,selectedSortFields));

       /* if ( indexNames != null )
            for(String indexName:indexNames)
                log.info(" index name = "+indexName);*/
        
      /*  if ( indexTypes != null )
            for(String indexType:indexTypes)
                log.info(" index type ="+ indexType);*/
        
       /* if ( selectedFields != null )
            for(String field:selectedFields)
                log.info(" selected field=" + field);
        
        if ( selectedSortFields != null )
            for(String field:selectedSortFields)
                log.info(" selected sort field=" + field);      
        
        if ( aggregationFields != null )                        
            for(String aField : aggregationFields)
                log.info(" aggregation field = "+aField); */
           
        if ( filterStr != null && !filterStr.trim().isEmpty() )
        {
            if ( filterStr.contains(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) || filterStr.contains(CommonKeys.CRITERIA_KEY_selectRelativeTime) )
                filterStr = replaceWithCurrentTime(filterStr,currentTime);
        }

        queryStr = processQueryStr(queryStr);
        
        qBuilder = getQueryBuilder(queryStr,filterStr);
        
        // builder
        SearchRequestBuilder builder = client.prepareSearch(indexNames);
        builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value

        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);
        
        if ( qBuilder != null )
            builder.setQuery(qBuilder);
                
        builder.setSize(maxExpectedHits);
        builder.setFrom(searchFrom);        
        
       /* if ( selectedFields == null || selectedFields.length == 0 )
            builder.addField("*"); 
        else
        {
            builder.addFields(selectedFields);
            builder.addFields("dataobject_type","target_repository_id","organization_id");
        }*/
       
        if ( selectedFields != null && selectedFields.length > 0 )
        {
            for(String filedName : selectedFields)
                builder.addStoredField(filedName);
                        
            builder.addStoredField("dataobject_type");
            builder.addStoredField("target_repository_id");
            builder.addStoredField("organization_id");
            builder.addStoredField("last_stored_time");
        }
        else
            builder.addStoredField("*");
                        
        if ( aggregationFields != null && aggregationFields.length > 0 )
        {
            for (String aggregationField : aggregationFields) // metadataname,90/ metadataname 80/
            {
                String[] aInfo = aggregationField.split("\\,");  // aInfo[0] - name, aInfo[1] - size, aInfo[2] - ASCEND
                termsBuilder = AggregationBuilders.terms(aInfo[0]+"_terms").field(aInfo[0]).order(Terms.Order.count(aInfo[2].equals("ASCEND"))).size(Integer.parseInt(aInfo[1]));              
                builder.addAggregation(termsBuilder);
            }
        }
        
        if ( selectedSortFields != null )
        {
            for(String sortField : selectedSortFields)
            {
                String[] str=sortField.split("\\,");

                if ( str.length != 2 || str[0].length()==0 || str[1].length()==0 )
                    break;

                if ( str[1].startsWith("ASC") )
                    sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.ASC);
                else
                    sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.DESC);

                builder.addSort(sBuilder);
            }
        }
        
        builder.setVersion(true); // return version for every search
        
        SearchResponse response = builder.execute().actionGet();           

        SearchHits shs = response.getHits();   
        //log.info("total hits = "+shs.getTotalHits());

        for (SearchHit hit : shs) 
        {
            searchResult = new HashMap<>(); 
            //searchResult.put("id", hit.getId()); 
            searchResult.put("id", String.format("%s%d",hit.getId(),hit.getVersion())); 
            searchResult.put("score", hit.getScore());
            searchResult.put("index", hit.getIndex());
            
            Map<String,SearchHitField> fields = hit.getFields();
            searchResult.put("fields", getResultInJsonForSearchHitField(fields)); 

            searchResults.add(searchResult);
        }
           
        result.put("searchResults", searchResults);
        result.put("totalHits", shs.getTotalHits());
        result.put("currentHits", searchResults.size());
                
        Aggregations aggregations = response.getAggregations();
        
        if ( aggregations != null )
            processAggregations(aggregations, result);  
        
        return result;
    }
    
    public static Map<String,Object> retrieveSimpleDataset(Client client, String[] indexNames, String[] indexTypes, String queryStr, String filterStr, int maxExpectedHits, int searchFrom, String[] selectedFields, String[] selectedSortFields,String[] aggregationFields,String currentTime)
    {
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> searchResults = new ArrayList<>();
        Map<String,Object> searchResult;
        //QueryStringQueryBuilder query = null;
        //FilterBuilder filter = null;
        SortBuilder sBuilder;
        TermsAggregationBuilder termsBuilder;
        QueryBuilder qBuilder;
        
        if ( filterStr != null && !filterStr.trim().isEmpty() )
        {
            if ( filterStr.contains(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) || filterStr.contains(CommonKeys.CRITERIA_KEY_selectRelativeTime) )
                filterStr = replaceWithCurrentTime(filterStr,currentTime);
        }

        queryStr = processQueryStr(queryStr);
        
        qBuilder = getQueryBuilder(queryStr,filterStr);
        
        // builder
        SearchRequestBuilder builder = client.prepareSearch(indexNames);
        builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value

        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);
        
        if ( qBuilder != null )
            builder.setQuery(qBuilder);
                
        builder.setSize(maxExpectedHits);
        builder.setFrom(searchFrom);        
        
      /*  if ( selectedFields == null || selectedFields.length == 0 )
            builder.addField("*"); 
        else
        {
            builder.addFields(selectedFields);
            //builder.addFields("dataobject_type","target_repository_id","organization_id");
        }*/
          
        if ( selectedFields != null && selectedFields.length > 0 )
        {
            for(String filedName : selectedFields)
              builder.addStoredField(filedName);
                        
            builder.addStoredField("dataobject_type");
            builder.addStoredField("target_repository_id");
            builder.addStoredField("organization_id");
            builder.addStoredField("last_stored_time");
        }
        else
            builder.addStoredField("*"); 
        
        if ( aggregationFields != null && aggregationFields.length > 0 )
        {
            for (String aggregationField : aggregationFields) // metadataname,90/ metadataname 80/
            {
                String[] aInfo = aggregationField.split("\\,");  // aInfo[0] - name, aInfo[1] - size, aInfo[2] - ASCEND
                termsBuilder = AggregationBuilders.terms(aInfo[0]+"_terms").field(aInfo[0]).order(Terms.Order.count(aInfo[2].equals("ASCEND"))).size(Integer.parseInt(aInfo[1]));              
                builder.addAggregation(termsBuilder);
            }
        }
        
        if ( selectedSortFields != null )
        {
            for(String sortField : selectedSortFields)
            {
                String[] str=sortField.split("\\,");

                if ( str.length != 2 || str[0].length()==0 || str[1].length()==0 )
                    break;

                if ( str[1].equals("ASCEND") )
                    sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.ASC);
                else
                    sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.DESC);

                builder.addSort(sBuilder);
            }
        }
        
        builder.setVersion(true); // return version for every search
        
        SearchResponse response = builder.execute().actionGet();           

        SearchHits shs = response.getHits();   
        //log.info("total hits = "+shs.getTotalHits());

        for (SearchHit hit : shs) 
        {
            searchResult = new HashMap<>(); 
            Map<String,Object> fieldMap = new HashMap<>();
            
            //searchResult.put("id", hit.getId()); 
            searchResult.put("id", String.format("%s%d",hit.getId(),hit.getVersion())); 
            searchResult.put("score", hit.getScore());
            searchResult.put("index", hit.getIndex());
            searchResult.put("indexType",hit.getType());
            searchResult.put("version", hit.getVersion());
            
            Map<String,SearchHitField> fields = hit.getFields();
            
            for (Map.Entry<String, SearchHitField> entry : fields.entrySet())
            {
                List<Object> values = entry.getValue().getValues();
                
                if ( values.size() == 1 )
                    fieldMap.put(entry.getKey(),values.get(0));   
                else
                    fieldMap.put(entry.getKey(),values);   
            }
            
            searchResult.put("fields", fieldMap); 

            searchResults.add(searchResult);
        }
           
        result.put("searchResults", searchResults);
        result.put("totalHits", shs.getTotalHits());
        result.put("currentHits", searchResults.size());
 
        return result;
    }
        
    public static Map<String,Object> retrieveAllDataset(Client client, String[] indexNames, String[] indexTypes, String queryStr, String filterStr, String[] selectedFields, String[] aggregationFields, int maxExpectedHits,String currentTime)
    {
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> searchResults = new ArrayList<>();
        Map<String,Object> searchResult;
        QueryBuilder qBuilder;
        TermsAggregationBuilder termsBuilder;

        log.debug(String.format("retrieveDataset: index names=(%s) index types = (%s) queryStr=(%s) filterStr=(%s) selectedFields=(%s)",indexNames,indexTypes,queryStr,filterStr,selectedFields));

       /* if ( indexNames != null )
            for(String indexName:indexNames)
                log.info(" index name = "+indexName);*/
        
      /*  if ( indexTypes != null )
            for(String indexType:indexTypes)
                log.info(" index type ="+ indexType);*/
        
       /* if ( selectedFields != null )
            for(String field:selectedFields)
                log.debug(" selected field=" + field);*/
           
        if ( filterStr != null && !filterStr.trim().isEmpty() )
        {
            if ( filterStr.contains(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) || filterStr.contains(CommonKeys.CRITERIA_KEY_selectRelativeTime) )
                filterStr = replaceWithCurrentTime(filterStr,currentTime);
        }

        queryStr = processQueryStr(queryStr);
        
        qBuilder = getQueryBuilder(queryStr,filterStr);
        
        // builder
        SearchRequestBuilder builder = client.prepareSearch(indexNames);

        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);

        builder.setQuery(qBuilder);     
        
        if ( aggregationFields != null && aggregationFields.length > 0 )
        {
            for (String aggregationField : aggregationFields) // metadataname,90/ metadataname 80/
            {
                String[] aInfo = aggregationField.split("\\,");  // aInfo[0] - name, aInfo[1] - size, aInfo[2] - ASCEND
                termsBuilder = AggregationBuilders.terms(aInfo[0]+"_terms").field(aInfo[0]).order(Terms.Order.count(aInfo[2].equals("ASCEND"))).size(Integer.parseInt(aInfo[1]));              
                builder.addAggregation(termsBuilder);
            }
        }
              
     /*   if ( selectedFields == null || selectedFields.length == 0 )
            builder.addField("*"); 
        else
        {
            builder.addFields(selectedFields);
            builder.addFields("dataobject_type","target_repository_id","organization_id");
        }*/
                    
        if ( selectedFields != null && selectedFields.length > 0 )
        {
            for(String filedName : selectedFields)
                builder.addStoredField(filedName);
                        
            builder.addStoredField("dataobject_type");
            builder.addStoredField("target_repository_id");
            builder.addStoredField("organization_id");
            builder.addStoredField("last_stored_time");
        }
        else
            builder.addStoredField("*");
                
        builder.setVersion(true); // return version for every search
        
        builder.setScroll(new TimeValue(120*1000));
 
        //builder.setSearchType(SearchType.SCAN); // default value
        builder.setSize(1000);
                 
        SearchResponse response = builder.execute().actionGet();      
        
        //Scroll until no hits are returned
        int thisTimeTotalHits = 0;
        long totalHits = 0;
        
        while (true) 
        {
            SearchHits shs = response.getHits();
            totalHits = shs.getTotalHits();
            
            log.info("total hits = "+totalHits+ "  current hits="+shs.getHits().length);

            for (SearchHit hit : shs) 
            {
                thisTimeTotalHits ++;
                
                searchResult = new HashMap<>(); 
                searchResult.put("id", String.format("%s%d",hit.getId(),hit.getVersion())); 
                searchResult.put("score", hit.getScore());
                searchResult.put("index", hit.getIndex());

                Map<String,SearchHitField> fields = hit.getFields();
                searchResult.put("fields", getResultInJsonForSearchHitField(fields)); 

                searchResults.add(searchResult);
                                                
                if ( thisTimeTotalHits >= maxExpectedHits && maxExpectedHits > 0 )
                {
                    result.put("searchResults", searchResults);
                    result.put("totalHits", shs.getTotalHits());
                    result.put("currentHits", thisTimeTotalHits);

                    return result;
                }
            }
            
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(120*1000)).execute().actionGet();
                        
            if ( response.getHits().getHits().length == 0 )
                break;
        }
        
        result.put("searchResults", searchResults);
        result.put("totalHits", totalHits);
        result.put("currentHits", thisTimeTotalHits);
          
        Aggregations aggregations = response.getAggregations();
        
        if ( aggregations != null )
            processAggregations(aggregations, result);  
         
        return result;
    }
    
    public static Map<String,Object> retrieveAllDatasetWithScroll(int fetchSize,int keepLiveSeconds, Client client, String[] indexNames, String[] indexTypes, String queryStr, String filterStr, String[] selectedFields, String[] selectedSortFields,String currentTime)
    {
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> searchResults = new ArrayList<>();
        Map<String,Object> searchResult;
        QueryBuilder qBuilder;
        SearchResponse response;
        SortBuilder sBuilder;

        log.info(String.format("retrieveDataset: index names=(%s) index types = (%s) queryStr=(%s) filterStr=(%s) selectedFields=(%s)",indexNames,indexTypes,queryStr,filterStr,selectedFields));

       /* if ( indexNames != null )
            for(String indexName:indexNames)
                log.info(" index name = "+indexName);

        if ( indexTypes != null )
            for(String indexType:indexTypes)
                log.info(" index type ="+ indexType);

        if ( selectedFields != null )
            for(String field:selectedFields)
                log.debug(" selected field=" + field);*/

        if ( filterStr != null && !filterStr.trim().isEmpty() )
        {
            if ( filterStr.contains(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) || filterStr.contains(CommonKeys.CRITERIA_KEY_selectRelativeTime) )
                filterStr = replaceWithCurrentTime(filterStr,currentTime);
        }

        queryStr = processQueryStr(queryStr); log.info(" query str= ="+queryStr);

        qBuilder = getQueryBuilder(queryStr,filterStr);

        // builder
        SearchRequestBuilder builder = client.prepareSearch(indexNames);
        
       /* if ( selectedSortFields == null || selectedSortFields.length == 0)
        {
            //builder.setSearchType(SearchType.SCAN); // default value
            builder.setSearchType(SearchType.DEFAULT); // default value
            builder.setSize(fetchSize/5);
        }
        else
        {
            builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value
            builder.setSize(fetchSize);
        }*/
            
        builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value
        builder.setSize(fetchSize);
            
        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);

        builder.setQuery(qBuilder);     

       /* if ( selectedFields == null || selectedFields.length == 0 )
            builder.addField("*"); 
        else
        {
            builder.addFields(selectedFields);
            builder.addFields("dataobject_type","target_repository_id","organization_id","last_stored_time");
        }
        */
            
        if ( selectedFields != null && selectedFields.length > 0 )
        {
            for(String filedName : selectedFields)
              builder.addStoredField(filedName);
                        
            builder.addStoredField("dataobject_type");
            builder.addStoredField("target_repository_id");
            builder.addStoredField("organization_id");
            builder.addStoredField("last_stored_time");
        }
        else
            builder.addStoredField("*");
        
        if ( selectedSortFields != null )
        {
            for(String sortField : selectedSortFields)
            {
                String[] str=sortField.split("\\,");

                if ( str.length != 2 || str[0].length()==0 || str[1].length()==0 )
                    break;

                if ( str[1].toUpperCase().startsWith("ASC") )
                    sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.ASC);
                else
                    sBuilder = SortBuilders.fieldSort(str[0].trim()).order(SortOrder.DESC);

                builder.addSort(sBuilder);
            }
        }
                
        builder.setVersion(true); // return version for every search

        builder.setScroll(new TimeValue(keepLiveSeconds*1000));

        response = builder.execute().actionGet();
        
        //Scroll until no hits are returned
        int thisTimeTotalHits = 0;
        long totalHits = 0;
        
        SearchHits shs = response.getHits();
        totalHits = shs.getTotalHits();

        log.info("total hits = "+totalHits+ " current hits="+shs.getHits().length);

        int retry = 0;
        
        while( totalHits > 0 && shs.getHits().length == 0 && retry<5  )
        {
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(keepLiveSeconds*1000)).execute().actionGet();
            shs = response.getHits();
            Tool.SleepAWhileInMS(250);
            
            log.warn("5555555555555555 get zero remaining objects. retry ="+retry+" for scrollId="+response.getScrollId());
             
            retry++;
        }
        
       /* if ( shs.getHits().length == 0 )
        {
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(keepLiveSeconds*1000)).execute().actionGet();
            shs = response.getHits();
        }*/
        
        for (SearchHit hit : shs)
        {
            thisTimeTotalHits ++;

            searchResult = new HashMap<>(); 
            searchResult.put("id", String.format("%s%d",hit.getId(),hit.getVersion())); 
            searchResult.put("score", hit.getScore());
            searchResult.put("index", hit.getIndex());
  
            Map<String,SearchHitField> fields = hit.getFields();
            searchResult.put("fields", getResultInJsonForSearchHitField(fields)); 

            Map<String,Object> fieldMap = new HashMap<>();
            for (Map.Entry<String, SearchHitField> entry : fields.entrySet())
            {
                List<Object> values = entry.getValue().getValues();
                
                if ( values.size() == 1 )
                    fieldMap.put(entry.getKey(),values.get(0));   
                else
                    fieldMap.put(entry.getKey(),values);   
            }
            
            searchResult.put("fieldMap", fieldMap); 
            
            searchResults.add(searchResult);
        }

        result.put("searchResults", searchResults);
        result.put("totalHits", totalHits);
        result.put("currentHits", thisTimeTotalHits);
        result.put("scrollId",response.getScrollId());
        
        return result;
    }
    
    public static Map<String,Object> retrieveRemainingDataset(String scrollId,int keepLiveSeconds, Client client)
    {
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> searchResults = new ArrayList<>();
        Map<String,Object> searchResult;
        SearchResponse response;

        log.debug(" scrollId="+scrollId+" keepLiveSeconds="+keepLiveSeconds);
        response = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(keepLiveSeconds*1000)).execute().actionGet();
        
        int thisTimeTotalHits = 0;
        long totalHits = 0;
        
        SearchHits shs = response.getHits();
        totalHits = shs.getTotalHits();

        log.debug("total hits = "+totalHits+ "  current hits="+shs.getHits().length);

        int retry = 0;
        
        while(shs.getHits().length == 0 && retry<5 )
        {
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(keepLiveSeconds*1000)).execute().actionGet();
            shs = response.getHits();
            Tool.SleepAWhileInMS(250);
            log.warn("5555555555555555 get zero remaining objects. retry ="+retry+" for scrollId="+response.getScrollId());
            
            retry++;
        }
        
        for (SearchHit hit : shs)
        {
            thisTimeTotalHits ++;

            searchResult = new HashMap<>(); 
            searchResult.put("id", String.format("%s%d",hit.getId(),hit.getVersion())); 
            searchResult.put("score", hit.getScore());
            searchResult.put("index", hit.getIndex());

            Map<String,SearchHitField> fields = hit.getFields();
            searchResult.put("fields", getResultInJsonForSearchHitField(fields)); 

            searchResults.add(searchResult);
        }

        result.put("searchResults", searchResults);
        result.put("totalHits", totalHits);
        result.put("currentHits", thisTimeTotalHits);
        result.put("scrollId",response.getScrollId());
        
        return result;
    }
    
    public static List<Map<String,String>> retrieveDatasetAggregation(Client client, String[] indexNames, String[] indexTypes, String queryStr, String filterStr, int maxExpectedHits, int searchFrom, String[] groupbyFields,String[] aggregationFieldsWithPrecision,String[] orderbyFields,String currentTime)
    {
        List<Map<String,String>> dataset;
        Map<String,String> record;
        
        QueryBuilder qBuilder = null;
        TermsAggregationBuilder termsBuilder;
        TermsAggregationBuilder lastTermsBuilder = null;
        TermsAggregationBuilder currentTermsBuilder;    
        boolean columnOrderIsAsc;
        String columnName;
        String aggrName;
        Map<String,Integer> aggregationFieldPrecisionMap = new HashMap<>();
        String[] aggregationFields = new String[aggregationFieldsWithPrecision.length];
        int scale;
        double doubleVal;
                
        for(int i=0;i<aggregationFieldsWithPrecision.length;i++)
        {
            String[] vals = aggregationFieldsWithPrecision[i].split("\\,");
            String fName = vals[0];
            
            if ( fName.contains("sum") || fName.contains("avg") || fName.contains("max") || fName.contains("min") )
            {
                fName = vals[0].substring(vals[0].indexOf("(")+1, vals[0].indexOf(")"));
            }
            
            if ( vals.length > 1 && Integer.parseInt(vals[1])>0 )
                aggregationFieldPrecisionMap.put(fName, Integer.parseInt(vals[1]) );
            else
                aggregationFieldPrecisionMap.put(fName, 2);
            
            aggregationFields[i] = vals[0];
        } 
        
        //for(String indexName:indexNames)
        //    log.info(" index name = "+indexName);
        
        //for(String indexType:indexTypes)
        //    log.info(" index type ="+ indexType);
        
        for(String field:groupbyFields)
            log.debug(" selected groupbyFields =" + field);
        
        for(String field:aggregationFields)
            log.debug(" selected aggregationFields=" + field);        
        
        for(String field:orderbyFields)
            log.debug(" selected orderbyFields=" + field);
               
        if ( filterStr != null && !filterStr.trim().isEmpty() )
        {
            if ( filterStr.contains(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) || filterStr.contains(CommonKeys.CRITERIA_KEY_selectRelativeTime) )
                filterStr = replaceWithCurrentTime(filterStr,currentTime);
        }
          
        queryStr = processQueryStr(queryStr);
        
        qBuilder = getQueryBuilder(queryStr,filterStr);
         
        SearchRequestBuilder builder = client.prepareSearch(indexNames);  
        
        builder.setSearchType(SearchType.QUERY_THEN_FETCH); // default value

        if ( indexTypes.length > 0 )
            builder.setTypes(indexTypes);
        
        if ( qBuilder != null )
           builder = builder.setQuery(qBuilder);
                        
        if ( groupbyFields.length > 0 )
        {
            columnOrderIsAsc = isColumnOrderDirectoryASC(orderbyFields,groupbyFields[0]);
            termsBuilder = AggregationBuilders.terms(groupbyFields[0]+"_terms").field(groupbyFields[0]).order(Terms.Order.term(columnOrderIsAsc)).missing(CommonKeys.ELASTIC_SEARCH_NULL_VALUE);
            builder.addAggregation(termsBuilder);
            lastTermsBuilder = termsBuilder; 
            
            for(int i=1;i<groupbyFields.length;i++)
            { 
                columnOrderIsAsc = isColumnOrderDirectoryASC(orderbyFields,groupbyFields[i]);      
                //currentTermsBuilder = AggregationBuilders.terms(groupbyFields[i]+"_terms").field(groupbyFields[i]).order(Terms.Order.term(columnOrderIsAsc)).size(0);
              
                currentTermsBuilder = AggregationBuilders.terms(groupbyFields[i]+"_terms").field(groupbyFields[i]).order(Terms.Order.term(columnOrderIsAsc)).missing(CommonKeys.ELASTIC_SEARCH_NULL_VALUE); // waiting for 2.1.1
                lastTermsBuilder.subAggregation(currentTermsBuilder);
                lastTermsBuilder = currentTermsBuilder;
            }
        }
    
        for(String aggregationField : aggregationFields)
        {
            if ( !aggregationField.contains("(") )
                continue;
            
            String fieldName = aggregationField.substring(aggregationField.indexOf("(")+1,aggregationField.indexOf(")"));

            if ( aggregationField.equals("count(*)") )
            {
                if ( groupbyFields.length <= 0 )
                    builder.addAggregation(AggregationBuilders.count("dataobject_type_count").field("dataobject_type"));
            }
            else
            if ( aggregationField.startsWith("count_distinct") )
            {
                if (  lastTermsBuilder == null )  // no groupby
                    builder.addAggregation(AggregationBuilders.cardinality(fieldName+"_count_distinct").field(fieldName));
                else
                    lastTermsBuilder.subAggregation(AggregationBuilders.cardinality(fieldName+"_count_distinct").field(fieldName));
            }    
            else
            if ( aggregationField.startsWith("count") )
            {
                if (  lastTermsBuilder == null )  // no groupby
                    builder.addAggregation(AggregationBuilders.count(fieldName+"_count").field(fieldName));
                else
                    lastTermsBuilder.subAggregation(AggregationBuilders.count(fieldName+"_count").field(fieldName));
            }        

            else
            if ( aggregationField.startsWith("sum") )
            {
                if (  lastTermsBuilder == null )  // no groupby
                    builder.addAggregation(AggregationBuilders.sum(fieldName+"_sum").field(fieldName));
                else
                    lastTermsBuilder.subAggregation(AggregationBuilders.sum(fieldName+"_sum").field(fieldName));
            }
            else
            if ( aggregationField.startsWith("avg") )
            {
                if (  lastTermsBuilder == null )  // no groupby
                    builder.addAggregation(AggregationBuilders.avg(fieldName+"_avg").field(fieldName));
                else
                    lastTermsBuilder.subAggregation(AggregationBuilders.avg(fieldName+"_avg").field(fieldName));
            }     
            else
            if ( aggregationField.startsWith("max") )
            {
                if (  lastTermsBuilder == null )  // no groupby
                    builder.addAggregation(AggregationBuilders.max(fieldName+"_max").field(fieldName));
                else
                    lastTermsBuilder.subAggregation(AggregationBuilders.max(fieldName+"_max").field(fieldName));
            }          
            else
            if ( aggregationField.startsWith("min") )
            {
                if (  lastTermsBuilder == null )  // no groupby
                    builder.addAggregation(AggregationBuilders.min(fieldName+"_min").field(fieldName));
                else
                    lastTermsBuilder.subAggregation(AggregationBuilders.min(fieldName+"_min").field(fieldName));
            }
        }           
        
        SearchResponse response = builder.execute().actionGet();           
 
        Aggregations aggregations = response.getAggregations();
  
        dataset = new ArrayList<>();
        
        if ( aggregations != null )
        {
            log.debug("total aggregation = "+ aggregations.asList().size()+", groupbyFields.length="+groupbyFields.length);
        
            if ( groupbyFields.length <= 0 )  // no groupby columns
            {
                record = new HashMap<>();

                for(Aggregation aggr : aggregations )
                {
                    aggrName = aggr.getName();

                    if ( aggrName.endsWith("_count") )
                    {
                        ValueCount count = (ValueCount)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_count"));

                        if ( columnName.equals("dataobject_type") )
                            columnName = "count(*)";
                        else
                            columnName = "count("+columnName+")";

                        record.put(columnName, String.valueOf(count.getValue()));
                    }
                    else
                    if ( aggrName.endsWith("_count_distinct") )
                    {
                        Cardinality countDistinct = (Cardinality)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_count_distinct"));
                        columnName = "count_distinct("+columnName+")";

                        record.put(columnName, String.valueOf(countDistinct.getValue()));
                    }
                    else
                    if ( aggrName.endsWith("_sum") )
                    {
                        Sum sum = (Sum)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_sum"));

                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(sum.getValue(), scale, BigDecimal.ROUND_HALF_UP);
                        
                        record.put("sum("+columnName+")", String.valueOf(doubleVal));
                    }
                    else
                    if ( aggrName.endsWith("_avg") )
                    {
                        Avg avg = (Avg)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_avg"));
                        
                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(avg.getValue(), scale, BigDecimal.ROUND_HALF_UP);

                        record.put("avg("+columnName+")", String.valueOf(doubleVal));
                    }     
                    else
                    if ( aggrName.endsWith("_max") )
                    {
                        Max max = (Max)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_max"));
 
                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(max.getValue(), scale, BigDecimal.ROUND_HALF_UP);
                        
                        record.put("max("+columnName+")", String.valueOf(doubleVal));
                    }  
                    else
                    if ( aggrName.endsWith("_min") )
                    {
                        Min min = (Min)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_min"));
                        
                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(min.getValue(), scale, BigDecimal.ROUND_HALF_UP);

                        record.put("min("+columnName+")", String.valueOf(doubleVal));
                    }                
                }

                if ( dataset.size() < maxExpectedHits )
                    dataset.add(record);
                else
                    return dataset;
            }
            else  // having groupby columns
            {
                generateAggregationResult(dataset,(Terms)aggregations.asList().get(0), new ArrayList<String[]>(), maxExpectedHits,aggregationFieldPrecisionMap);
            }
        }
        else
             log.info("no aggregation info!!! ");
        
        return dataset;
    }
    
    private static String processQueryStrForSearchDocument(String queryStr)
    {
        //return queryStr;
        
        String newQueryStr = "";
        
        if ( queryStr == null )
            return newQueryStr;
        
        String[] vals = queryStr.split(" ");
        
        for(int i=0; i<vals.length; i++)
        {
            if ( vals[i] == null || vals[i].trim().isEmpty() )
                continue;
            
            if ( !vals[i].contains(":") && !vals[i].contains("AND") && !vals[i].contains("OR") && !vals[i].contains("(") && 
                    !vals[i].contains(")") && !vals[i].contains("[") && !vals[i].contains("]") && !vals[i].contains("TO") && !vals[i].contains("\"") )
                vals[i]=vals[i].trim().concat("*");
            
            String str = vals[i].trim();
            String newStr = "";
            
            if ( str.startsWith("\"") && str.endsWith("\"") )
                newStr = str;
            else
            {                          
                for(int j=0;j<str.length();j++)
                {
                    char c = str.charAt(j);

                    if ( Tool.isChineseChar(c) )
                        newStr += String.valueOf(c)+"* ";
                    else
                        newStr += String.valueOf(c);
                }
                
                if ( newStr.endsWith(" *") )
                    newStr = newStr.substring(0,newStr.length()-2);
            } 
            
            newQueryStr += " " + newStr +" ";
        }
        
        return newQueryStr;
    }
      
    private static String processQueryStr(String queryStr)
    {
        //return queryStr;
        
        String newQueryStr = "";
        
        if ( queryStr == null )
            return newQueryStr;
        
        String[] vals = queryStr.split(" ");
        
        for(int i=0; i<vals.length; i++)
        {
            if ( vals[i] == null || vals[i].trim().isEmpty() )
                continue;
            
            if ( !vals[i].contains(":") && !vals[i].contains("AND") && !vals[i].contains("OR") && !vals[i].contains("(") && 
                    !vals[i].contains(")") && !vals[i].contains("[") && !vals[i].contains("]") && !vals[i].contains("TO") && !vals[i].contains("\"") )
                vals[i]=vals[i].trim().concat("*");
                        
            String str = vals[i].trim();
            String newStr = "";
            
            if ( str.startsWith("\"") && str.endsWith("\"") )
                newStr = str;
            else
            {                          
                for(int j=0;j<str.length();j++)
                {
                    char c = str.charAt(j);

                    if ( Tool.isChineseChar(c) )
                        newStr += String.valueOf(c)+"* ";
                    else
                        newStr += String.valueOf(c);
                }
                
                if ( newStr.endsWith(" *") )
                    newStr = newStr.substring(0,newStr.length()-2);
            } 
            
            newQueryStr += " " + newStr +" ";
        }
        
        return newQueryStr;
    }
      
    private static QueryStringQueryBuilder getQueryBuilder(String queryStr,String filterStr)
    {
        QueryStringQueryBuilder qBuilder = null; 
        
        if ( filterStr != null && !filterStr.trim().isEmpty() && queryStr != null && !queryStr.trim().isEmpty() )
            qBuilder = QueryBuilders.queryStringQuery(String.format("( %s ) AND ( %s )",queryStr.trim(),filterStr.trim()));
        else        
        if ( filterStr != null && !filterStr.trim().isEmpty() && ( queryStr == null || queryStr.trim().isEmpty()) )
            qBuilder = QueryBuilders.queryStringQuery(filterStr.trim());
        else
        if ( queryStr != null && !queryStr.trim().isEmpty() && ( filterStr == null || filterStr.trim().isEmpty()) )
            qBuilder = QueryBuilders.queryStringQuery(queryStr.trim());
        
        return qBuilder;
    }
    
    private static void processAggregations(Aggregations aggregations, Map<String, Object> result) 
    {
        log.info("total aggregation = "+ aggregations.asList().size());
        
        StringBuilder strBuilder = new StringBuilder();
        
        for(Aggregation  aggr : aggregations.asList())
        {
            Terms terms =(Terms)aggr;
            String fieldName = terms.getName().substring(0, terms.getName().lastIndexOf("_"));
            
            for(Bucket bucket : terms.getBuckets())
                strBuilder.append(String.format("%s(%d),%s,%d",fieldName,terms.getBuckets().size(),bucket.getKey(),bucket.getDocCount())).append(";");
        }
        
        String newStr = strBuilder.toString();
        
        if ( !newStr.isEmpty() )
            newStr = newStr.substring(0, newStr.length()-1);
        
        result.put("aggregationResult", newStr);
    }

    private static void generateAggregationResult(List<Map<String,String>> result,Terms termsAggr,List<String[]> parentTermsInfo, int maxExpectedHits,Map<String,Integer> aggregationFieldPrecisionMap)
    {
        String termsAggrName;
        String columnName;
        String aggrName;
        Map<String,String> record;
        int scale;
        double doubleVal;
                                
        termsAggrName = termsAggr.getName();
        
        //log.info(" generateAggregationResult() termsAggrName="+termsAggrName+", termsAggr.getBuckets().size="+termsAggr.getBuckets().size());
        
        for(Bucket b:termsAggr.getBuckets())
        {                         
            record = null;        
            
            if ( b.getAggregations().asList().isEmpty() )
            {
                record = new HashMap<>();

                for(String[] parentTerms: parentTermsInfo)
                    record.put(parentTerms[0], parentTerms[1]);

                //record.put(termsAggrName.substring(0,termsAggrName.lastIndexOf("_")),b.getKey());          
                record.put(termsAggrName.substring(0,termsAggrName.lastIndexOf("_")),(String)b.getKey());    
                record.put("count(*)", String.valueOf(b.getDocCount()));  
            }
            
            for( Aggregation aggr : b.getAggregations().asList() )
            {
                aggrName = aggr.getName();
                
                if ( aggrName.endsWith("terms") )
                {
                    parentTermsInfo.add(new String[]{termsAggrName.substring(0,termsAggrName.lastIndexOf("_")),(String)b.getKey()});
                    generateAggregationResult(result,(Terms)aggr, parentTermsInfo, maxExpectedHits,aggregationFieldPrecisionMap);
                    
                    if ( result.size() == maxExpectedHits )
                        return;
                }
                else
                {
                    if ( record == null)
                    {
                        record = new HashMap<>();
            
                        for(String[] parentTerms: parentTermsInfo)
                            record.put(parentTerms[0], parentTerms[1]);
                        
                        record.put(termsAggrName.substring(0,termsAggrName.lastIndexOf("_")),(String)b.getKey());                       
                        record.put("count(*)", String.valueOf(b.getDocCount()));                        
                    }
            
                    if ( aggrName.endsWith("_count") )
                    {
                        ValueCount count = (ValueCount)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_count"));

                        if ( columnName.equals("dataobject_type") )
                            columnName = "count(*)";
                        else
                            columnName = "count("+columnName+")";

                        record.put(columnName, String.valueOf(count.getValue()));
                    }
                    else
                    if ( aggrName.endsWith("_count_distinct") )
                    {
                        Cardinality countDistinct = (Cardinality)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_count_distinct"));
                        columnName = "count_distinct("+columnName+")";

                        record.put(columnName, String.valueOf(countDistinct.getValue()));
                    }
                    else
                    if ( aggrName.endsWith("_sum") )
                    {
                        Sum sum = (Sum)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_sum"));

                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(sum.getValue(), scale, BigDecimal.ROUND_HALF_UP);
                        
                        record.put("sum("+columnName+")", String.valueOf(doubleVal));
                    }
                    else
                    if ( aggrName.endsWith("_avg") )
                    {
                        Avg avg = (Avg)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_avg"));

                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(avg.getValue(), scale, BigDecimal.ROUND_HALF_UP);
                        
                        record.put("avg("+columnName+")", String.valueOf(doubleVal));
                    }     
                    else
                    if ( aggrName.endsWith("_max") )
                    {
                        Max max = (Max)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_max"));
                        
                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(max.getValue(), scale, BigDecimal.ROUND_HALF_UP);
                        
                        record.put("max("+columnName+")", String.valueOf(doubleVal));
                    }  
                    else
                    if ( aggrName.endsWith("_min") )
                    {
                        Min min = (Min)aggr;
                        columnName = aggrName.substring(0,aggrName.indexOf("_min"));
          
                        scale = aggregationFieldPrecisionMap.get(columnName);
                        doubleVal = Tool.convertDouble(min.getValue(), scale, BigDecimal.ROUND_HALF_UP);
                        
                        record.put("min("+columnName+")", String.valueOf(doubleVal));
                    }                                                
                }
            }
            
            if ( record != null )
            {
                result.add(record);
                
                if ( result.size() == maxExpectedHits )
                    return;
            }
        }
    }
    
    private static boolean isColumnOrderDirectoryASC(String[] sourceColumns,String targetColumn)
    {
        for(String field:sourceColumns)
        {
            if ( field.startsWith(targetColumn) )
            {
                return field.substring(field.indexOf(",")+1).trim().equals("ASC");
            }
        }
        
        return true;  // default ASC
    }
    
    public static XContentBuilder createTypeMapping(String indexTypeName,List<Map<String,String>> indexTypeFieldDefinition) throws Exception 
    {
        String name, dataType, fieldIndexType, inSearchResult,boost;
        boolean inSearchResultValue = true;
        float boostVal;
        
        XContentBuilder mapping;
            
        try
        {
            log.info("createTypeMapping() ...");
 
            mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(indexTypeName)
                .startObject("_source").field("enabled", true).endObject()
                .field("dynamic","strict")
                .startObject("properties");
                      
            for(Map<String,String> fieldDefinition : indexTypeFieldDefinition)
            {                
                name = fieldDefinition.get("name");
                dataType = fieldDefinition.get("dataType");
                fieldIndexType =  fieldDefinition.get("indexType");
                //inSearchResult =  fieldDefinition.get("inSearchResult");
                boost = fieldDefinition.get("searchFactor");

                if ( name == null || dataType == null)
                    return null;
 
                if ( dataType.equals("binary"))
                    continue;
                                                
                    //if ( fieldIndexType == null )
                //    indexTypeStr = "analyzed";
                //else
                //    indexTypeStr = MetadataIndexType.findByValue(Integer.parseInt(fieldIndexType)).getIndexType();
                
                //if ( inSearchResult == null )
                //    inSearchResultValue = false;
                //else
                //    inSearchResultValue = inSearchResult.trim().equals("1");
                
                if ( boost == null )
                    boostVal = 1.0f;
                else
                    boostVal = Float.valueOf(boost);
                 
                if ( dataType.equals("string") )
                    mapping.startObject(name).field("type","text").field("store", inSearchResultValue).field("index",true).field("boost",boostVal).endObject();
                else
                    mapping.startObject(name).field("type",dataType).field("store", inSearchResultValue).field("index",true).field("boost",boostVal).endObject();                  
            
                //String mappingStr = String.format(" name=%s dataType=%s store=%b indexType=%s boostVal=%f",name,dataType,inSearchResultValue, boostVal);
                //log.debug("mappingStr = "+mappingStr);
            }
                           
            mapping.endObject()
                .endObject()
                .endObject();
        } 
        catch (Exception e) {
            log.error("failed to createTypeMapping! e="+e);
            throw e;
        }
        
        log.info("created mapping for indexType:"+indexTypeName+", mapping="+mapping);

        return mapping;
    }
    
    public static XContentBuilder createTypeMapping(EntityManager platformEm, DataobjectType dataobjectType, boolean withDataobjectFields) throws Exception 
    {
        Node name, dataType, indexType, inSearchResult,boost;
        String nameStr, dataTypeStr, indexTypeStr;  
        boolean inSearchResultValue = true;
        boolean indexTypeBool;
        float boostVal;
        
        XContentBuilder mapping;
        List<Element> list = new ArrayList<>();

        try
        {
            log.info("create mapping dataobjectType="+dataobjectType.getIndexTypeName());
 
            String analyzer = Util.getSystemConfigValue(platformEm,0,"system","es_analyzer"); log.info("111111111 analyzer="+analyzer);
             
            mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(dataobjectType.getIndexTypeName())
                .startObject("_source").field("enabled", true).endObject()
                .field("dynamic","strict")
                .startObject("properties");
                         
            getElement(platformEm, dataobjectType, list, withDataobjectFields); // get all parent type element
        
            for( Element element: list)
            {                
                name = element.selectSingleNode("name");
                dataType = element.selectSingleNode("dataType");
                indexType = element.selectSingleNode("indexType");
                inSearchResult = element.selectSingleNode("inSearchResult");
                boost = element.selectSingleNode("searchFactor");

                if ( name == null || dataType == null)
                    return null;
                
                nameStr = name.getText().trim();
                dataTypeStr = MetadataDataType.findByValue(Integer.parseInt(dataType.getText().trim())).getEsType();
                
                if ( dataTypeStr.equals("binary"))
                    continue;
                                
                if ( indexType == null )
                    indexTypeStr = "analyzed";
                else
                    indexTypeStr = MetadataIndexType.findByValue(Integer.parseInt(indexType.getText().trim())).getIndexType();
                                
                if ( boost == null )
                    boostVal = 1.0f;
                else
                    boostVal = Float.valueOf(boost.getText());
                            
                if ( dataTypeStr.equals("string") )
                {
                    if ( indexTypeStr.equals("no"))
                        mapping.startObject(nameStr).field("type","text").field("store", true).field("index",false).field("boost",boostVal).endObject();
                    else
                    {
                        if ( analyzer.isEmpty() ) // no analyzer
                        {
                            mapping.startObject(nameStr).field("type","text").field("store", true).field("index",true).field("boost",boostVal).startObject("fields").startObject("keyword").field("type","keyword").endObject().endObject().endObject();
                        }
                        else
                        {
                            log.info("222222222 analyzer="+analyzer);
                            mapping.startObject(nameStr).field("type","text").field("store", true).field("index",true).field("analyzer",analyzer).field("boost",boostVal).startObject("fields").startObject("keyword").field("type","keyword").endObject().endObject().endObject();
                        }
                    }
                }
                else
                {
                    if ( indexTypeStr.equals("no") )
                        indexTypeBool = false;
                    else
                        indexTypeBool = true;
                    
                    mapping.startObject(nameStr).field("type",dataTypeStr).field("store", true).field("index",indexTypeBool).field("boost",boostVal).endObject();
                }
                //String mappingStr = String.format(" namestr=%s dataTypeStr=%s store=%b boostVal=%f",nameStr,dataTypeStr,true,boostVal);
                
                //log.info("mappingStr = "+mappingStr);
            }
            
            addContentMapping(mapping);  // add content mapping to all dataobject type
                             
            mapping.endObject()
                .endObject()
                .endObject();
        } 
        catch (Exception e) {
            log.error("failed to createTypeMapping! e="+e);
            throw e;
        }
        
        log.info("created mapping dataobjectType="+dataobjectType.getIndexTypeName()+" mapping="+mapping);

        return mapping;
    }

    private static void addContentMapping(XContentBuilder mapping) throws Exception
    {
        mapping.startObject("contents").startObject("properties")
                .startObject("content_id").field("type","text").field("store",true).field("index",true).endObject()
                .startObject("content_type").field("type","integer").field("store",true).field("index",true).endObject()
                .startObject("content_name").field("type","text").field("store",true).field("index",true).endObject()
               // .startObject("content_text").field("type","text").field("store",false).field("index",true).field("term_vector","with_positions_offsets").field("indexAnalyzer","ik").field("searchAnalyzer","ik").endObject()         
                .startObject("content_text").field("type","text").field("store",false).field("index",true).endObject()
                .startObject("second_content_text").field("type","text").field("store",false).field("index",true).endObject()  
                .startObject("mime_type").field("type","text").field("store",true).field("index",true).endObject()    
                .startObject("encoding").field("type","text").field("store",true).field("index",true).endObject()
                .startObject("is_content_in_system").field("type","boolean").field("store",true).field("index",true).endObject()          
                .startObject("content_size").field("type","long").field("store",true).field("index",true).endObject()
                .startObject("content_tags").field("type","text").field("store",true).field("index",true).endObject()
                .startObject("content_location").field("type","text").field("store",true).field("index",true).endObject()
                .startObject("similar_contents").startObject("properties")
                    .startObject("content_id").field("type","text").field("store",false).field("index",true).endObject()   
                    .startObject("score").field("type","float").field("store",false).field("index",true).endObject()
                .endObject().endObject()
                .startObject("content_info").startObject("properties")
                    .startObject("key").field("type","text").field("store",false).field("index",true).endObject()   
                    .startObject("value").field("type","text").field("store",false).field("index",true).endObject()
                .endObject().endObject()
                .startObject("processing_info").startObject("properties")
                    .startObject("processing_type_id").field("type","integer").field("store",false).field("index",true).endObject()   
                    .startObject("processed_time").field("type","date").field("store",false).field("index",true).endObject()
                    .startObject("result").field("type","text").field("store",false).field("index",true).endObject()
                .endObject().endObject()
                .endObject().endObject();
    }
    
    private static void getElement(EntityManager platformEm,DataobjectType dataobjectType, List<Element> elementList, boolean withDataobjectFields) 
    {
        if (dataobjectType.getParentType() > 0 ) // has parent 
            getElement(platformEm, Util.getDataobjectType(platformEm,dataobjectType.getParentType()),elementList,withDataobjectFields);
        
        if ( withDataobjectFields == false && dataobjectType.getParentType()==0 )
            return;
        
        Document metadataXmlDoc = Tool.getXmlDocument(dataobjectType.getMetadatas());   
        
        List<Element> list = metadataXmlDoc.selectNodes(CommonKeys.METADATA_NODE);
        
        for(Element element:list)
            elementList.add(element);
    }
    
    private static String getHighlightsInJson(Map<String, HighlightField> highlightFields) 
    {        
        XContentBuilder mapping;
        StringBuilder fieldText;
        
        try
        {
            mapping = XContentFactory.jsonBuilder()
                    .startObject().startArray("highlights");

            for(Map.Entry<String,HighlightField> entry:highlightFields.entrySet())
            {
                mapping.startObject();
                
                HighlightField titleField = entry.getValue();
                fieldText = new StringBuilder();
                
                if ( titleField!= null)
                {
                    Text[] titleTexts = titleField.fragments();  
                    for(Text text : titleTexts)
                        fieldText.append(text).append("; ");
                }
                
                mapping.field("highlight_field",(String)entry.getKey());
                mapping.field("highlight_text",fieldText.toString());
                
                //log.info("highligh text ="+fieldText.toString());
                
                mapping.endObject();
            }
            
            mapping.endArray().endObject();
            
            return mapping.string();
        } 
        catch (Exception e) 
        {
            log.error("failed to getResultInJson! e="+e);
            return null;
        }
    }
        
    private static String getResultInJsonForSearchHitField(Map<String, SearchHitField> fields) 
    {        
        XContentBuilder mapping;
        
        try
        {
            mapping = XContentFactory.jsonBuilder().startObject();
                         
            for (Map.Entry<String, SearchHitField> entry : fields.entrySet())
            {
                List<Object> values = entry.getValue().getValues();
                
                if ( values.size() == 1)
                    mapping.field(entry.getKey(),values.get(0));
                else
                    mapping.field(entry.getKey(),values);
            }
            mapping.endObject();
            
            return mapping.string();
        } 
        catch (Exception e) 
        {
            log.error("failed to getResultInJsonForSearchHitField()! e="+e);
            return null;
        }
    }
    
    private static String getResultInJsonForGetField(Map<String, GetField> fields) 
    {        
        XContentBuilder mapping;
        
        try
        {
            mapping = XContentFactory.jsonBuilder().startObject();
                         
            for (Map.Entry<String, GetField> entry : fields.entrySet())
            {
                List<Object> values = entry.getValue().getValues();
                
                if ( values.size() == 1)
                    mapping.field(entry.getKey(),values.get(0));
                else
                    mapping.field(entry.getKey(),values);
            }
            mapping.endObject();
            
            return mapping.string();
        } 
        catch (Exception e) 
        {
            log.error("failed to getResultInJsonForGetField()! e="+e);
            return null;
        }
    }
    
    private static String replaceWithCurrentTime(String filterStr,String currentTime)
    {
        StringBuilder builder = new StringBuilder(); 
        Date startDate;
        Date endDate;
        int start;
        int end;
        int relativeTimeType;
        String timeStr = "";
        int relativeTime;
        int timeUnit;
        String tmp;
        String startDateStr,endDateStr;
        Calendar cal = Calendar.getInstance();   
        Calendar firstDayOfThisMonth;
        Calendar firstDayOfThisYear;
        
        if ( currentTime != null )
        {
            endDate = Tool.convertStringToDate(currentTime, "yyyy-MM-dd HH:mm:ss");
            
            if ( endDate == null )
                endDate = new Date();
        }
        else
            endDate = new Date();
        
        if ( filterStr.contains(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) ) //( ( file_type.last_accessed_time:[predefineRelativeTimeType-4])
        {
            end = 0;
            
            while(true)
            {
                start = filterStr.indexOf(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime, end);
                
                if ( start == -1 )
                {
                    builder.append(filterStr.substring(end));
                    break;
                }
                
                builder.append(filterStr.substring(end, start));
                
                end = filterStr.indexOf("]", start);
                relativeTimeType = Integer.parseInt(filterStr.substring(start+CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime.length()+1, end));
                log.info("start ="+start+" end="+end+" relativeTimeType="+relativeTimeType);
                        
                switch(RelativeTimeType.findByValue(relativeTimeType))
                {
                    case TODAY:
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate); 
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",Tool.convertDateToDateStringForES(endDate),endDateStr);
                        break;
                    case WEEK_TO_DATE:
                    case BUSINESS_WEEK_TO_DATE:
                        cal.setTime(new Date(endDate.getTime()));
                        Calendar firstDayOfThisWeek = Tool.getFirstDayOfWeek(cal);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfThisWeek.getTime());
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);
                        break;      
                    case MONTH_TO_DATE:
                        //endDate = new Date();
                        cal.setTime(new Date(endDate.getTime()));
                        firstDayOfThisMonth = Tool.getFirstDayOfMonth(cal);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfThisMonth.getTime());
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);
                        break;       
                    case THIS_MONTH:
                        //endDate = new Date();
                        cal.setTime(new Date(endDate.getTime()));
                        firstDayOfThisMonth = Tool.getFirstDayOfMonth(cal);
                        endDate = Tool.endOfMonth(endDate);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfThisMonth.getTime());
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);
                        break;  
                    case THIS_YEAR:
                        //endDate = new Date();
                        cal.setTime(new Date(endDate.getTime()));
                        firstDayOfThisYear = Tool.getFirstDayOfYear(cal);
                        endDate = Tool.endOfYear(endDate);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfThisYear.getTime());
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);
                        break;                          
                    case YEAR_TO_DATE:
                        cal.setTime(new Date(endDate.getTime()));
                        firstDayOfThisYear = Tool.getFirstDayOfYear(cal);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfThisYear.getTime());
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);
                        break;   
                    case YESTERDAY:
                        endDate = new Date(endDate.getTime()-24*3600*1000);
                        endDateStr = Tool.convertDateToDateEndStringForES(endDate);              
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",Tool.convertDateToDateStringForES(endDate),endDateStr);                        
                        break;             
                    case PREVIOUS_WEEK:
                    case PREVIOUS_BUSINESS_WEEK:
                        //endDate = new Date();
                        cal.setTime(new Date(endDate.getTime()-7*24*3600*1000));
                        Calendar firstDayOfLastWeek = Tool.getFirstDayOfWeek(cal);
                        Calendar lastDayOfLastWeek = Tool.getLastDayOfWeek(cal);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfLastWeek.getTime());
                        endDateStr = Tool.convertDateToDateEndStringForES(lastDayOfLastWeek.getTime());
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;   
                    case PREVIOUS_MONTH:
                        //endDate = new Date();
                        cal.setTime(endDate);
                        Calendar firstDayOfLastMonth = Tool.getFirstDayOfLastMonth(cal);
                        Calendar lastDayOfLastMonth = Tool.getLastDayOfLastMonth(cal);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfLastMonth.getTime());
                        endDateStr = Tool.convertDateToDateEndStringForES(lastDayOfLastMonth.getTime());
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;           
                    case PREVIOUS_YEAR:
                        //endDate = new Date();
                        cal.setTime(endDate);
                        Calendar firstDayOfLastYear = Tool.getFirstDayOfLastYear(cal);
                        Calendar lastDayOfLastYear = Tool.getLastDayOfLastYear(cal);
                        startDateStr = Tool.convertDateToDateStringForES(firstDayOfLastYear.getTime());
                        endDateStr = Tool.convertDateToDateEndStringForES(lastDayOfLastYear.getTime());
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;                           
                    case LAST_15_MINUTES:
                        //endDate = new Date();
                        startDate = new Date(endDate.getTime()-15*60*1000);
                        startDateStr = Tool.convertDateToTimestampStringForES(startDate);
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;       
                    case LAST_60_MINUTES:
                        //endDate = new Date();
                        startDate = new Date(endDate.getTime()-60*60*1000);
                        startDateStr = Tool.convertDateToTimestampStringForES(startDate);
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;          
                    case LAST_4_HOURS:
                        //endDate = new Date();
                        startDate = new Date(endDate.getTime()-4*3600*1000);
                        startDateStr = Tool.convertDateToTimestampStringForES(startDate);
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;            
                    case LAST_24_HOURS:
                        //endDate = new Date();
                        startDate = new Date(endDate.getTime()-24*3600*1000);
                        startDateStr = Tool.convertDateToTimestampStringForES(startDate);
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;      
                    case LAST_7_DAYS:
                        //endDate = new Date();
                        startDate = new Date(endDate.getTime()-7*24*3600*1000);
                        startDateStr = Tool.convertDateToTimestampStringForES(startDate);
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;     
                    case LAST_30_DAYS:
                        //endDate = new Date();
                        startDate = new Date(endDate.getTime()-15*24*3600*1000);   // some java bugs here  , canot minus 30 days
                        startDate = new Date(startDate.getTime()-15*24*3600*1000);
                        startDateStr = Tool.convertDateToTimestampStringForES(startDate);
                        endDateStr = Tool.convertDateToTimestampStringForES(endDate);
                        endDateStr = Tool.getEndDatetime(endDateStr);
                        timeStr = String.format("%s TO %s",startDateStr,endDateStr);                       
                        break;                          
                    default:
                        timeStr = "* TO *";
                }
                
                builder.append(timeStr);
            }
        }
        else
        if ( filterStr.contains(CommonKeys.CRITERIA_KEY_selectRelativeTime) ) // file_type.created_time:[relativeTimeType-4-4]
        {
            end = 0;
            
            while(true)
            {
                start = filterStr.indexOf(CommonKeys.CRITERIA_KEY_selectRelativeTime, end);
                
                if ( start == -1 )
                {
                    builder.append(filterStr.substring(end));
                    break;
                }
                
                builder.append(filterStr.substring(end, start));
                
                end = filterStr.indexOf("]", start);
                tmp = filterStr.substring(start+CommonKeys.CRITERIA_KEY_selectRelativeTime.length()+1, end); //4-4
                relativeTime = Integer.parseInt(tmp.substring(0, tmp.indexOf("-")));
                timeUnit = Integer.parseInt(tmp.substring(tmp.indexOf("-")+1));
                
                log.info("start ="+start+" end="+end+" relativeTimeType="+relativeTime+" timeunit="+timeUnit+" tmp="+tmp);
                
                //endDate = new Date();
                long diff = TimeUnit.findByValue(timeUnit).getSeconds()*1000*relativeTime;
                startDate = new Date(endDate.getTime() - diff);
                
                timeStr = String.format("%s TO %s",Tool.convertDateToTimestampStringForES(startDate),Tool.convertDateToTimestampStringForES(endDate));
                
                log.info("timeStr ="+timeStr);
                
                builder.append(timeStr);
            }
        }
        
        log.debug(" replaced filterStr ="+builder.toString());
        
        return builder.toString();
    }
                    
    public static boolean ifSnapshortRepositoryExist(Client client,String snapshotRepositoryName)
    {
        GetRepositoriesRequestBuilder getRepo = new GetRepositoriesRequestBuilder(client.admin().cluster(),GetRepositoriesAction.INSTANCE);
        
        GetRepositoriesResponse repositoryMetaDatas = getRepo.execute().actionGet();
   
        for( RepositoryMetaData data : repositoryMetaDatas.repositories() )
        {
            if ( data.name().equals(snapshotRepositoryName) )
                return true;
        }
         
        return false;
    }
    
    public static void createSnapshotRepository(Client client,String snapshotRepositoryName,String snapshotLocation) 
    {
        Settings settings = Settings.builder()
                .put("location", snapshotLocation)
                .build();      
                         
        PutRepositoryRequestBuilder putRepo = 
                    new PutRepositoryRequestBuilder(client.admin().cluster(),PutRepositoryAction.INSTANCE);
        
        putRepo.setName(snapshotRepositoryName)
                .setType("fs")
                .setSettings(settings)
                .execute().actionGet();
    }
    
    public static void createSnapshot(Client client,String snapshotRepositoryName, String snapshotPrefix, String[] indexPatternToSnapshot,boolean waitForComplete) 
    {
        CreateSnapshotRequestBuilder builder = new CreateSnapshotRequestBuilder(client.admin().cluster(),CreateSnapshotAction.INSTANCE);
        
        String snapshot = snapshotPrefix + "-" +Tool.convertDateToString(new Date(), "yyyy-MM-dd-HHmmss");
        
        builder.setRepository(snapshotRepositoryName)
            .setIndices(indexPatternToSnapshot)
            .setWaitForCompletion(waitForComplete)
            .setSnapshot(snapshot);            
        
        builder.execute().actionGet();
    }
    
    public static void deleteSnapshot(Client client,String snapshotRepositoryName,String snapshot) 
    {
        try
        {
            DeleteSnapshotRequestBuilder builder = new DeleteSnapshotRequestBuilder(client.admin().cluster(),DeleteSnapshotAction.INSTANCE);
            builder.setRepository(snapshotRepositoryName).setSnapshot(snapshot);
            builder.execute().actionGet();
        } 
        catch (Exception e) 
        {
            log.error("failed to deleteSnapshot()! e="+e);
        }
    }
    
    public static void deleteSnapshotRepository(Client client,String snapshotRepositoryName) 
    {
        try
        {
            DeleteRepositoryRequestBuilder builder = new DeleteRepositoryRequestBuilder(client.admin().cluster(),DeleteRepositoryAction.INSTANCE);
            builder.setName(snapshotRepositoryName);
            builder.execute().actionGet();
        } 
        catch (Exception e) 
        {
            log.error("failed to deleteSnapshotRepository()! e="+e);
        }
    }
}
   