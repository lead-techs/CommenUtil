/*
 * DeleteAndRetentionUtil .java
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManager;
import java.util.Date;
import java.util.HashMap;
import javax.persistence.Query;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.TypeMissingException;
import org.json.JSONObject;

import com.broaddata.common.model.enumeration.DataobjectStatus;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.MetadataIndexType;
import com.broaddata.common.model.enumeration.SQLDBSyncStatus;
import com.broaddata.common.model.enumeration.SearchScope;
import com.broaddata.common.model.enumeration.SearchType;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.IndexSet;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import com.broaddata.common.thrift.dataservice.SearchResponse;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;

public class DeleteAndRetentionUtil 
{      
    static final Logger log = Logger.getLogger("DeleteAndRetentionUtil");     
     
    public static long deleteDataobjectTypeFromRepository(EntityManager platformEm, EntityManager em, int repositoryId, int dataobjectTypeId, String filter) 
    {
        DataobjectType dataobjectType;
        String indexTypeName;
        Client esClient;
        String sql;
        Query query;
        String indexName;
        String dataobjectId;
        List<String> indexNameList = new ArrayList<>();
        List<String> indexTypeList = new ArrayList<>();
        long count = 0;
        BulkResponse bulkResponse = null; 
        
        try 
        {
            log.info(" deleting dataobject_type="+dataobjectTypeId);
            
            dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
            indexTypeName = dataobjectType.getIndexTypeName().trim();
                        
            log.info(" start to get es clinet! ");
            
            esClient = ESUtil.getClient(em, platformEm, repositoryId, false);
                        
            log.info(" finish get es clinet! ");
            
            indexTypeList.add(dataobjectType.getIndexTypeName());
            
            if ( filter == null )
                filter = "";
            
            while(true)
            {
                log.info("1111 getting es data .... ");
                
                Map<String, Object> searchRet = ESUtil.retrieveDataset(esClient, new String[]{}, indexTypeList.toArray(new String[1]),
                        "", filter, 6000, 0, new String[]{"dataobject_type"}, new String[]{}, new String[]{},null);
                
                int currentHits = (Integer) searchRet.get("currentHits");
                
                log.info("1111 finish getting es data! currentHits="+currentHits);
                
                if (currentHits <= 0) {
                    break;
                }
                
                List<Map<String, Object>> searchResults = (List<Map<String, Object>>) searchRet.get("searchResults");
                
                BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        
                for (Map<String, Object> result : searchResults) 
                {
                    dataobjectId = (String) result.get("id");
                    dataobjectId = dataobjectId.substring(0, 40);
                    indexName = (String) result.get("index");

                    bulkRequest.add(esClient.prepareDelete(indexName, indexTypeName, dataobjectId));
            
                    //DeleteResponse res = esClient.prepareDelete(indexName, indexTypeName, dataobjectId)
                    //        .execute()
                    //        .actionGet();

                    count++;
                    
                    //log.info(String.format("deleting indexType[%s] dataobject[%s] from index[%s]", indexTypeName, dataobjectId, indexName));
                }
                
                int retry = 0;
                
                while(true)
                {
                    retry++;
                    
                    try
                    {
                        log.info("1111 bulk delete data .... ");
                         
                        bulkResponse = ESUtil.sendBulkRequest(bulkRequest);
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error("sendBuildRequest for deleting faield! e="+e);
                        Tool.SleepAWhile(3, 0);
                        
                        if ( retry > 10 )
                            throw e;
                    }
                }                
               
                log.info("1111 finish bulk delete data .... ");
                 
             /*   BulkItemResponse[] responses = bulkResponse.getItems();

                for(BulkItemResponse res : responses)
                {
                    if ( res.isFailed() )
                    {
                        String msg = String.format("delete failed! itemId=%d isFailed=%b index=%s index_type=%s docId=%s failure=%s",res.getItemId(),res.isFailed(),res.getIndex(),res.getType(),res.getId(),res.getFailure());
                        log.error(msg);
                    }
                }*/
            }            
        } 
        catch (Exception e) 
        {
            log.error("deleteDataobjectTypeFromRepository() failed! e="+e);
            throw e;
        }
        
        return count;
    }
    
    public static long deleteDataBySQL(int organizationId, int repositoryId, int catalogId, String sqlLikeStr) 
    {
        String indexTypeName;
        Client esClient;
        String indexName;
        String dataobjectId;
        List<String> indexNameList = new ArrayList<>();
        List<String> indexTypeList = new ArrayList<>();
        long count = 0;
        BulkResponse bulkResponse = null;
        
        try
        {
            Map<String,String> paramters = SearchUtil.getParametersFromDeleteSQLLikeString(sqlLikeStr);
            
            indexTypeName = paramters.get("indexTypeName");
            String filter = paramters.get("filter");
            
            esClient =  ESUtil.getClient(organizationId, repositoryId, false);
            indexTypeList.add(indexTypeName);
            
            while (true) 
            {
                Map<String, Object> searchRet = ESUtil.retrieveDataset(esClient, new String[]{}, indexTypeList.toArray(new String[1]),
                        "", filter, 6000, 0, new String[]{"dataobject_type"}, new String[]{}, new String[]{},null);
                
                int currentHits = (Integer) searchRet.get("currentHits");
                
                if (currentHits <= 0) {
                    break;
                }
                
                List<Map<String, Object>> searchResults = (List<Map<String, Object>>) searchRet.get("searchResults");
                
                BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        
                for (Map<String, Object> result : searchResults) 
                {
                    dataobjectId = (String) result.get("id");
                    dataobjectId = dataobjectId.substring(0, 40);
                    indexName = (String) result.get("index");

                    bulkRequest.add(esClient.prepareDelete(indexName, indexTypeName, dataobjectId));
 
                    count++;
                    
                    log.debug(String.format("deleting indexType[%s] dataobject[%s] from index[%s]", indexTypeName, dataobjectId, indexName));
                }
                
                int retry = 0;
                
                while(true)
                {
                    retry++;
                    
                    try
                    {
                        bulkResponse = ESUtil.sendBulkRequest(bulkRequest);
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error("sendBuildRequest for deleting faield! e="+e);
                        Tool.SleepAWhile(3, 0);
                        
                        if ( retry > 10 )
                            throw e;
                    }
                }                
               
                BulkItemResponse[] responses = bulkResponse.getItems();

                for(BulkItemResponse res : responses)
                {
                    if ( res.isFailed() )
                    {
                        String msg = String.format("delete failed! itemId=%d isFailed=%b index=%s index_type=%s docId=%s failure=%s",res.getItemId(),res.isFailed(),res.getIndex(),res.getType(),res.getId(),res.getFailure());
                        log.error(msg);
                    }
                }
            }            
        } 
        catch (Exception e) 
        {
            log.error("deleteDataobjectTypeFromRepository() failed! e="+e);
            throw e;
        }
        
        return count;
    }
  
    public static long reSyncDataToSQLDB(int organizationId,EntityManager platformEm, EntityManager em, int repositoryId, int dataobjectTypeId) throws Exception 
    {
        DataobjectType dataobjectType;
        String indexTypeName;
        Client esClient;
        Client configEsClient;
        String sql;
        String indexName;
        String dataobjectId;
        List<String> indexNameList = new ArrayList<>();
        List<String> indexTypeList = new ArrayList<>();
        long count = 0;
        BulkResponse bulkResponse = null;
        Map<String,Object> fieldMap;
        JSONObject record = null;
        long oldVersion = 0;
        Map<String, Object> jsonMap;
        Date currentTime; 
        String sytemIndexName;
        
        try 
        {
            Repository configRrepository = Util.getConfigRepository(em,organizationId);
            configEsClient = ESUtil.getClient(em, platformEm, configRrepository.getId(), false);
                        
            esClient = ESUtil.getClient(em, platformEm, repositoryId, false);
                
            dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
            indexTypeName = dataobjectType.getIndexTypeName().trim();
       
            sytemIndexName = String.format("edf_system_config_%d",organizationId);
            String recordId = String.format("%d_%d_%s",organizationId,repositoryId,indexTypeName);
            
            try
            {
                //record = ESUtil.findRecord(esClient,indexName,CommonKeys.SQLDB_SYNC_INFO_INDEX_TYPE,recordId);
                //oldVersion = (long)record.get("version");
                
                Map<String,Object>  ret = ESUtil.findRecordWithVersion(configEsClient,sytemIndexName,CommonKeys.SQLDB_SYNC_INFO_INDEX_TYPE,recordId);
                record = (JSONObject)ret.get("jsonObject");
                oldVersion = (long)ret.get("version");
            }
            catch(Exception e)
            {
                log.error(" findRecord failed! e="+e);
            }
            
            if ( record == null )
            {
                log.error(" sync info not avaiable! return 0!");
                oldVersion = 0;
            }
            else
            {
                int status = record.optInt("status");
                
                if ( status != SQLDBSyncStatus.SYNCHRONIZED.getValue()  )
                {
                    log.info(" status is not synchronized, return -1!");
                    return -1;
                }
            }
            
            currentTime = new Date(); 

            jsonMap = new HashMap<>();

            jsonMap.put("organization_id", organizationId);
            jsonMap.put("repository_id",repositoryId);
            jsonMap.put("index_type_name", dataobjectType.getIndexTypeName());
            jsonMap.put("status",SQLDBSyncStatus.RESETING_SYNC_STATUS.getValue());
            jsonMap.put("status_time",currentTime);
            jsonMap.put("last_update_time",currentTime);
            jsonMap.put("items_needed_to_sync", 0);

            while(true)
            {
                try
                {            
                    oldVersion = ESUtil.persistRecord(configEsClient, sytemIndexName, CommonKeys.SQLDB_SYNC_INFO_INDEX_TYPE, recordId, jsonMap,oldVersion);
                    break;
                }
                catch(Exception e)
                {
                    log.info(" lock and update record failed! e="+e+" indextype="+dataobjectType.getIndexTypeName());

                    if ( e instanceof TypeMissingException )
                    {
                        List<Map<String,String>> definition = Util.prepareSQLDBSyncInfoIndexTypeDefinition();
                        XContentBuilder typeMapping = ESUtil.createTypeMapping(CommonKeys.SQLDB_SYNC_INFO_INDEX_TYPE,definition);
                        ESUtil.putMappingToIndex(esClient, sytemIndexName, CommonKeys.SQLDB_SYNC_INFO_INDEX_TYPE, typeMapping);
                        continue;
                    }

                    return -2;
                }
            }
                    
            indexTypeList.add(dataobjectType.getIndexTypeName());
            
            String queryStr = String.format("object_status:%d",DataobjectStatus.SYNCHRONIZED.getValue());  
                         
            while (true)
            {
                Map<String,Object> searchRet = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, indexTypeList.toArray(new String[1]),
                                queryStr, "", 7500, 0, new String[]{"object_status"}, new String[]{}, new String[]{},null);
                
                int currentHits = (Integer) searchRet.get("currentHits");
                
                if (currentHits <= 0) {
                    break;
                }
                 
                List<Map<String, Object>> searchResults = (List<Map<String, Object>>) searchRet.get("searchResults");
                
                BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        
                for (Map<String, Object> result : searchResults) 
                {
                    dataobjectId = (String) result.get("id");
                    dataobjectId = dataobjectId.substring(0, 40);
                    indexName = (String) result.get("index");
                    
                    //fieldMap = (Map<String,Object>)result.get("fields");
                    fieldMap = new HashMap<>();
                    fieldMap.put("object_status",DataobjectStatus.UNSYNCHRONIZED.getValue());    /////////////////////

                    bulkRequest.add(esClient.prepareUpdate().setIndex(indexName).setType(indexTypeName).setId(dataobjectId).setDoc(fieldMap));
                    //bulkRequest.add(esClient.prepareIndex(indexName, indexTypeName).setId(dataobjectId).setSource(fieldMap));

                    count++;
                }
                
                int retry = 0;
                
                while(true)
                {
                    retry++;
                    
                    try
                    {
                        bulkResponse = ESUtil.sendBulkRequest(bulkRequest);
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error("sendBuildRequest for resync faield! e="+e);
                        Tool.SleepAWhile(3, 0);
                        
                        if ( retry > 5 )
                            throw e;
                    }
                }                
               
                BulkItemResponse[] responses = bulkResponse.getItems();

                for(BulkItemResponse res : responses)
                {
                    if ( res.isFailed() )
                    {
                        String msg = String.format("resync failed! itemId=%d isFailed=%b index=%s index_type=%s docId=%s failure=%s",res.getItemId(),res.isFailed(),res.getIndex(),res.getType(),res.getId(),res.getFailure());
                        log.error(msg);
                    }
                }
                
                currentTime = new Date(); 
                jsonMap.put("last_update_time",currentTime);

                try
                {            
                    oldVersion = ESUtil.persistRecord(configEsClient, sytemIndexName, CommonKeys.SQLDB_SYNC_INFO_INDEX_TYPE, recordId, jsonMap,oldVersion);
                }
                catch(Exception e)
                {
                    log.info(" lock and update record failed! e="+e+" indextype="+dataobjectType.getIndexTypeName());
                    return -3;
                }
            }
            
            currentTime = new Date(); 
 
            jsonMap.put("status",SQLDBSyncStatus.NEED_TO_SYNC.getValue());
            jsonMap.put("status_time",currentTime);

            try
            {            
                oldVersion = ESUtil.persistRecord(configEsClient, sytemIndexName, CommonKeys.SQLDB_SYNC_INFO_INDEX_TYPE, recordId, jsonMap,oldVersion);
            }
            catch(Exception e)
            {
                log.info(" lock and update record failed! e="+e+" indextype="+dataobjectType.getIndexTypeName());
                return -3;
            }
        } 
        catch (Exception e) 
        {
            log.error("reSyncDataToSQLDB() failed! e="+e);
            throw e;
        }
        
        return count;
    }
 
    public static long reIndexData(int organizationId,EntityManager platformEm, EntityManager em, int repositoryId, int dataobjectTypeId) throws Exception 
    {
        DataobjectType dataobjectType;
        String indexTypeName;
        Client esClient;
        String indexName;
        List<String> indexNameList = new ArrayList<>();
        String dataobjectId;
        long count = 0;
        BulkResponse bulkResponse = null;
        Map<String,Object> fieldMap;    
        String scrollId;
        SearchResponse response; 
        List<Map<String,String>> searchResults;
        BulkRequestBuilder bulkRequest;
        int processed = 0;
        Map<String,String> stringTimeFieldNameMap = new HashMap<>();
        List<String> reIndexFieldNames = new ArrayList<>();
        List<Map<String,Object>> metadataDefinitions;
        String stringFieldName;
        String targetFieldName;
             
        try 
        {
            Repository repository = em.find(Repository.class, repositoryId);
          
            esClient = ESUtil.getClient(em, platformEm, repositoryId, false);
                
            dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
            indexTypeName = dataobjectType.getIndexTypeName().trim();                    
     
            SearchRequest request = new SearchRequest(organizationId,repositoryId,repository.getDefaultCatalogId(),SearchType.SEARCH_GET_ALL_RESULTS.getValue());

            int searchScope = dataobjectType.getIsEventData()==1?SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue():SearchScope.NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue();
            request.setSearchScope(searchScope);

            List<String> indexTypes = new ArrayList<>();
            indexTypes.add(dataobjectType.getIndexTypeName());

            request.setTargetIndexTypes(indexTypes);
            request.setSearchFrom(0); 
            request.setColumns("object_status");

            int retry = 0;

            while(true)
            {
                retry++;

                response = SearchUtil.getDataobjectsWithScroll(em,platformEm,request,3000,CommonKeys.KEEP_LIVE_SECOND);
                searchResults = response.getSearchResults();

                scrollId = response.getScrollId();

                if ( !searchResults.isEmpty() )
                     break;

                if ( retry > 5 )
                {
                    log.error("dataService.getDataobjectsWithScroll() get 0 result! exit! ");
                    break;
                }

                log.info(" dataService.getDataobjectsWithScroll() get 0 result! retry="+retry);
                Tool.SleepAWhile(1, 0);
            }

            if ( !searchResults.isEmpty() )
            {     
                metadataDefinitions = Util.getDataobjectTypeMetadataDefinition(platformEm,dataobjectTypeId, true, true);
             
                for(Map<String,Object> definition : metadataDefinitions)
                {
                    String name = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                    String dataType = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                    String datetimeFormat = definition.get(CommonKeys.METADATA_DATETIME_FORMAT)==null?"":(String)definition.get(CommonKeys.METADATA_DATETIME_FORMAT);
                    
                    String indexType = (String)definition.get(CommonKeys.METADATA_NODE_INDEX_TYPE);
                    
                    if ( Integer.valueOf(indexType) == MetadataIndexType.NOT_ANALYZED_AND_ANALYZED.getValue() && Integer.valueOf(dataType)== MetadataDataType.STRING.getValue())
                        reIndexFieldNames.add(name);
                    
                    if ( Integer.valueOf(dataType)== MetadataDataType.DATE.getValue() && name.endsWith("_DATE") || 
                        Integer.valueOf(dataType)== MetadataDataType.TIMESTAMP.getValue() && name.endsWith("_TIMESTAMP") )
                    {
                        stringTimeFieldNameMap.put(name,datetimeFormat);
                    }
                }
            }
            
            if ( stringTimeFieldNameMap.isEmpty() && reIndexFieldNames.isEmpty() )
            {
                log.info(" dataobject_type ="+dataobjectType.getName()+" doesn't have date/timestamp field from string. and no filed with both analyzed and not_analyzed");
                return 0;
            }
            
            while(true)
            {
                // process dataobjectVOs
                log.info(" 11111111111111111111 dataobjectVOs size="+searchResults.size());

                bulkRequest = esClient.prepareBulk();

                for(Map<String,String> searchResult : searchResults)
                {    
                    processed ++;
                    log.info(" processing line = "+processed);

                    //JSONObject resultFields = new JSONObject(searchResult.get("fields"));
 
                    dataobjectId = (String) searchResult.get("id");
                    dataobjectId = dataobjectId.substring(0, 40);
                    indexName = (String) searchResult.get("index");
                    
                    if ( !indexNameList.contains(indexName) )
                    {
                        retry = 0;
                        
                        while(true)
                        {
                            retry++;
                            
                            try
                            {
                                XContentBuilder typeMapping = ESUtil.createTypeMapping(platformEm, dataobjectType, true);
                                ESUtil.putMappingToIndex(esClient, indexName, dataobjectType.getIndexTypeName(), typeMapping);
                                
                                break;
                            }
                            catch(Exception e)
                            {
                                log.error(" create new mapping failed! e="+e);
                                
                                Tool.SleepAWhile(1, 0);
                                
                                if ( retry > 5 )
                                    throw new Exception(" create new mapping failed! e="+e);
                            }
                        }
                        
                        indexNameList.add(indexName);
                    }
                                    
                    fieldMap = ESUtil.getRecord(esClient,indexName,indexTypeName,dataobjectId);
                    
                    int k=0;
                    
                    for(Map.Entry<String,String> entry : stringTimeFieldNameMap.entrySet())
                    {
                        targetFieldName = entry.getKey();
                        stringFieldName = targetFieldName.substring(0,targetFieldName.lastIndexOf("_"));
                        
                        String datetimeFormat = entry.getValue();
                        
                        //String val = resultFields.optString(stringFieldName);
                        
                        String val = (String)fieldMap.get(stringFieldName);
                        
                        if ( val == null || val.trim().isEmpty() )
                            continue;
                                             
                        Date date = Tool.convertStringToDate(val, datetimeFormat);
                        
                        if ( date == null)
                            continue;
                       
                        k++;
                        
                        fieldMap.put(targetFieldName, date); 
                        
                        log.info(" set new date field! targetFiledName="+targetFieldName+" datetimeformat="+datetimeFormat+" val="+val);
                    }
                    
                    for(String fieldName : reIndexFieldNames)
                    {
                        String val = (String)fieldMap.get(fieldName);
                        
                        if ( val == null || val.isEmpty() )
                            continue;
                        
                        k++;
                        
                        fieldMap.put(fieldName, val); log.info(" reindex field name="+fieldName+" val="+val);
                    }
                    
                    if ( k > 0 )
                        bulkRequest.add(esClient.prepareIndex().setIndex(indexName).setType(indexTypeName).setId(dataobjectId).setSource(fieldMap));

                    count++;
                }
                              
                boolean failed;
                retry = 0;
                
                while(true)
                {
                    retry++;
                    
                    try
                    {
                        failed = false;
                        
                        if ( bulkRequest.numberOfActions() == 0 )
                            break;
                        
                        bulkResponse = ESUtil.sendBulkRequest(bulkRequest);
                        
                        BulkItemResponse[] responses = bulkResponse.getItems();

                        for(BulkItemResponse res : responses)
                        {
                            if ( res.isFailed() )
                            {
                                String msg = String.format("resync failed! itemId=%d isFailed=%b index=%s index_type=%s docId=%s failure=%s",res.getItemId(),res.isFailed(),res.getIndex(),res.getType(),res.getId(),res.getFailure());
                                log.error(msg);
                                
                                failed = true;
                                
                                if ( res.getFailure().getCause() instanceof TypeMissingException || res.getFailure().getCause() instanceof StrictDynamicMappingException)
                                {
                                    XContentBuilder typeMapping = ESUtil.createTypeMapping(platformEm, dataobjectType, true);
                                    ESUtil.putMappingToIndex(esClient, res.getIndex(), dataobjectType.getIndexTypeName(), typeMapping);
                                    break;
                                }
                            }
                        }
                
                        if ( !failed )
                            break;
                    }
                    catch(Exception e)
                    {
                        log.error("sendBuildRequest for resync faield! e="+e);
                        Tool.SleepAWhile(3, 0);
                        
                        if ( retry > 5 )
                            throw e;
                    }
                }                
               
                if ( response.hasMoreData )
                {
                    retry = 0;

                    while(true)
                    {
                        retry++;

                        response = SearchUtil.getRemainingDataobjects(em,platformEm,repository.getId(),scrollId,600);

                        log.info("1111111111111111 get search result! number ="+response.getSearchResults().size()+" hasMoreData="+response.hasMoreData);

                        if ( retry > 5 )
                        {
                            log.error("222222222222222222 failed to get remaining dataobject! retry="+retry);
                            break;
                        }

                        if ( response.getSearchResults().isEmpty() )
                        {
                            Tool.SleepAWhileInMS(500);
                            continue;
                        }

                        break;
                    }

                    searchResults = response.getSearchResults();
                }
                else
                    break;
            }      
        } 
        catch (Exception e) 
        {
            log.error("reProcessTimeField() failed! e="+e);
            throw e;
        }
        
        return count;
    }
    
    private static String getFieldDatetimeFormat(EntityManager em,int organizationId,int dataobjectTypeId,String stringFieldName)
    {
        String name = "";
        String xml = null;
        int datasourceId = 0;
        
        try
        {
            String sql = String.format("from Datasource where organizationId=%d ",organizationId);
            List<Datasource> datasourceList = em.createQuery(sql).getResultList();
            
            for(Datasource datasource : datasourceList)
            {
                xml =  datasource.getProperties();
          
                if ( !xml.contains("targetDataobjectType") )
                    continue;
                
                String str = xml.substring(xml.indexOf("<targetDataobjectType>")+"<targetDataobjectType>".length(), xml.indexOf("</targetDataobjectType>"));
                int targetDataobjectType = Integer.parseInt(str);
                
                if ( targetDataobjectType == dataobjectTypeId )
                {
                    datasourceId = datasource.getId();
                    break;
                }
            }
          
             
        }
        catch(Exception e)
        {
            log.error(" getDataobjectTypeDatasourceConnectionName() failed! e="+e);
        }
                    
        return name;
    }
    
}
