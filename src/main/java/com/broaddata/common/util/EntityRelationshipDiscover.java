
package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import java.util.HashMap;
import java.util.ArrayList;
import org.elasticsearch.client.Client;
import org.json.JSONObject;
import java.util.Calendar;
import java.util.Date;
  
import com.broaddata.common.model.organization.RelationshipDiscoveryTask;
import com.broaddata.common.model.platform.EntityType;
import com.broaddata.common.model.organization.RelationshipDiscoveryJob;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import com.broaddata.common.thrift.dataservice.SearchResponse;
import com.broaddata.common.model.enumeration.RelationshipDiscoveryType;
import com.broaddata.common.model.enumeration.SearchType;
import com.broaddata.common.model.enumeration.SearchScope;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.RelationshipType;
import com.broaddata.common.model.vo.DataobjectAssociatedData;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.model.enumeration.AssociationTypeProperty;
   
public class EntityRelationshipDiscover implements Serializable  // calucate all metrics of one entityId , no child entity
{
    static final Logger log = Logger.getLogger("EntityRelationshipDiscover");
            
    private int FETCH_SIZE = 1000;  // 每次取 的笔数
    private int KEEP_LIVE_SECONDS = 600; // 数据在服务端保持的秒数
            
    private RelationshipDiscoveryTask task;   
    private RelationshipDiscoveryJob job;
    private EntityType entityType;
    private String dataobjectId;
    private EntityManager em;
    private EntityManager platformEm;
    private ArrayList<Map<String,Object>> relationships;
    private List<ColumnMapping> columnMappingList;
    private Client esClient;
    private JSONObject dataobjectJson;
    private DataobjectType dataobjectType;
    private DataobjectType entityActionDataobjectType;
    private DataobjectType associatedDataobjectType;
    private Repository repository;
    private RelationshipType relationshipType;
    private List<String> primaryKeys;
    private EntityType discoveryEntityType;
    private List<String> discoveryPrimaryKeys;
    private String dataobjectName;
    private String dataobjectDescription;
    private RelationshipDiscoveryType discoveryType;
    private DataobjectType relatedDataobjectType;
    private String entityFieldName;
    private String relatedDataobjectTypeFieldName1;
    private String relatedDataobjectTypeFieldName2;
    private String discoveryEntityFiledName;
    private int relationshipTimes;
             
    public EntityRelationshipDiscover(Client esClient,DataobjectType dataobjectType,Repository repository,RelationshipDiscoveryTask task, RelationshipDiscoveryJob job,EntityType entityType, EntityManager em, EntityManager platformEm,List<String> primaryKeys,EntityType discoveryEntityType,List<String> discoveryPrimaryKeys) {
        this.task = task;
        this.entityType = entityType;
        this.em = em;
        this.platformEm = platformEm;
        this.job = job;
        this.dataobjectType = dataobjectType;
        this.repository = repository;
        this.esClient = esClient;
        this.primaryKeys = primaryKeys;
        this.discoveryPrimaryKeys = discoveryPrimaryKeys;
        this.discoveryEntityType = discoveryEntityType;
    }
 
    public void init()
    {
        String columnName;
        String dataobjectTypeName;
        int entityActionDataobjectTypeId;
 
        try
        {
            discoveryType = RelationshipDiscoveryType.findByValue(task.getDiscoveryType());
            
            relationshipType = platformEm.find(RelationshipType.class, task.getRelationshipType()); 
            
            // take each
            columnMappingList = new ArrayList<>();
            
            if ( discoveryType == RelationshipDiscoveryType.SAME_ENITY_TYPE_SAME_ATTRIBUTE_SAME_VALUE )
            {
                String[] vals = task.getDiscoveryColumnsMapping().split("\\,");  //T1:C1;T1:C1,...
            
                for(String val : vals ) // each mapping
                {
                    String[] vals1 = val.split("\\:");                  
                    
                    dataobjectTypeName = vals1[0];
                    columnName = vals1[1].substring(0, vals1[1].indexOf("-"));
                                           
                    boolean found = false;
                    
                    for(ColumnMapping mapping : columnMappingList)
                    {
                        if ( mapping.getDataobjectTypeName().trim().equals(dataobjectTypeName) )
                        {
                            found = true;
                            mapping.setColumnName(mapping.getColumnName()+","+columnName);
                            break;
                        }
                    }
                    
                    if ( !found )
                    {                    
                        ColumnMapping columnMapping = new ColumnMapping();

                        columnMapping.setDataobjectTypeName(dataobjectTypeName);
                        columnMapping.setColumnName(columnName);
                        columnMappingList.add(columnMapping);
                    }                        
                }
            }
            else
            if ( discoveryType == RelationshipDiscoveryType.SAME_ENITY_TYPE_DIFFERENT_ATTRIBUTE_SAME_VALUE )
            {
                String[] vals = task.getDiscoveryColumnsMapping().split("\\,");  // c1,c2
            
                ColumnMapping columnMapping = new ColumnMapping();
   
                String[] vals1 = vals[0].split("\\:");                  
                    
                dataobjectTypeName = vals1[0];
                columnName = vals1[1].substring(0, vals1[1].indexOf("-"));
                    
                columnMapping.setDataobjectTypeName(dataobjectTypeName);
                columnMapping.setColumnName(columnName);
                
                vals1 = vals[1].split("\\:");   
                dataobjectTypeName = vals1[0];
                columnName = vals1[1].substring(0, vals1[1].indexOf("-"));
                    
                columnMapping.setTargetDataobjectTypeName(dataobjectTypeName);
                columnMapping.setTargetColumnName(columnName);
  
                columnMappingList.add(columnMapping);
            }
            else
            if ( discoveryType == RelationshipDiscoveryType.SAME_ENITY_TYPE_SAME_ACTION )
            {
                String[] vals = task.getDiscoveryColumnsMapping().split("\\,");  //1,entityActionDataobjectTypeId,T1:C1;T1:C1 
                relationshipTimes = Integer.parseInt(vals[0]);
                entityActionDataobjectTypeId = Integer.parseInt(vals[1]);
                            
                entityActionDataobjectType = platformEm.find(DataobjectType.class, entityActionDataobjectTypeId);
                int i = 0;
                
                for(String val : vals ) // each mapping
                {
                    i++;
                    
                    log.info("1111 val="+val);
                    
                    if ( i<=2 )
                       continue;
                    
                    log.info("222 val="+val);
                    
                    ColumnMapping columnMapping = new ColumnMapping();
                    columnMapping.setEntityActionDataojbectTypeId(entityActionDataobjectTypeId);

                    String[] vals1 = val.split("\\:");                  
                    
                    dataobjectTypeName = vals1[0];log.info("dataobjectType="+dataobjectTypeName);
                    columnName = vals1[1].substring(0, vals1[1].indexOf("-"));log.info("columnName="+columnName);
                    
                    log.info("dataobjectType="+dataobjectTypeName);
                    columnMapping.setDataobjectTypeName(dataobjectTypeName);
                    columnMapping.setColumnName(columnName);
  
                    columnMappingList.add(columnMapping);
                }
            }
            else
            if ( discoveryType == RelationshipDiscoveryType.SAME_ENITY_TYPE_SAME_ACTION_RELATED )
            {
                String[] vals = task.getDiscoveryColumnsMapping().split("\\,");  // c1,c2
                entityActionDataobjectTypeId = Integer.parseInt(vals[0]);
                                     
                entityActionDataobjectType = platformEm.find(DataobjectType.class, entityActionDataobjectTypeId);
                
                ColumnMapping columnMapping = new ColumnMapping();
                columnMapping.setEntityActionDataojbectTypeId(entityActionDataobjectTypeId);
   
                columnMapping.setDataobjectTypeName(dataobjectType.getName());
                columnMapping.setColumnName(vals[1]);

                columnMapping.setTargetDataobjectTypeName(dataobjectType.getName());
                columnMapping.setTargetColumnName(vals[2]);

                columnMappingList.add(columnMapping);
            }
            else
            if ( discoveryType == RelationshipDiscoveryType.DIFFERENT_ENITY_TYPE_DIFFERENT_ATTRIBUTE_SAME_VALUE )
            {
                                                 
                EntityType associatedEntityType = platformEm.find(EntityType.class, task.getDiscoveryEntityType());
                associatedDataobjectType = platformEm.find(DataobjectType.class, associatedEntityType.getDataobjectType());           
            }
            else
            if ( discoveryType == RelationshipDiscoveryType.DIFFERENT_ENITY_TYPE_IN_SAME_TABLE )
            {
                EntityType associatedEntityType = platformEm.find(EntityType.class, task.getDiscoveryEntityType());
                associatedDataobjectType = platformEm.find(DataobjectType.class, associatedEntityType.getDataobjectType());  
            
                String[] vals = task.getDiscoveryColumnsMapping().split("\\,");  
                int relatedDataobjectTypeId = Integer.parseInt(vals[0]);
                relatedDataobjectType = Util.getDataobjectType(platformEm, relatedDataobjectTypeId);
                 
                entityFieldName = vals[1].substring(0, vals[1].indexOf("-"));
                relatedDataobjectTypeFieldName1 = vals[2].substring(0, vals[2].indexOf("-"));
                relatedDataobjectTypeFieldName2 = vals[3].substring(0, vals[3].indexOf("-"));
                discoveryEntityFiledName =  vals[4].substring(0, vals[4].indexOf("-"));
            }   
        }
        catch(Exception e)
        {
            log.error("init() failed! e="+e);
            throw e;
        }
    }
    
    public boolean setupDataobjectId(String dataobjectId) 
    {
        try
        {
            this.dataobjectId = dataobjectId;
        
            dataobjectJson = ESUtil.getDocument(esClient,task.getOrganizationId(), dataobjectId, dataobjectType.getIndexTypeName());
            
            if ( dataobjectJson == null )
                return false;
            
            dataobjectName = Util.getDataobjectTypePrimaryKeyValue(primaryKeys,dataobjectJson,null);  
            
            dataobjectDescription = Util.getDataobjectTypeFieldValue(entityType.getDataobjectTypeNameFields(),dataobjectJson,null);  
            
            return true;
         }
        catch(Exception e)
        {
            log.error("init() failed! e="+e);
            throw e;
        }
    }
 
    public ArrayList<Map<String,Object>> discoveryRelationships() throws Exception
    {
        relationships = new ArrayList<>();
        
        try
        {        
            switch( discoveryType )
            {
                case SAME_ENITY_TYPE_SAME_ATTRIBUTE_SAME_VALUE:
                    processSameEntityTypeSameAttributeSameValueRelationship();
                    break;
                case SAME_ENITY_TYPE_DIFFERENT_ATTRIBUTE_SAME_VALUE:
                    processSameEntityTypeDifferentAttributeSameValueRelationship();
                    break;
                case SAME_ENITY_TYPE_SAME_ACTION:
                    processSameEntityTypeSameActionRelationship();
                    break;
                case SAME_ENITY_TYPE_SAME_ACTION_RELATED:
                    processSameEntityTypeSameActionRelatedRelationship();
                    break;        
                case DIFFERENT_ENITY_TYPE_DIFFERENT_ATTRIBUTE_SAME_VALUE:
                    processDifferentEntityTypeDifferentAttributeRelationship();
                    break;  
                case DIFFERENT_ENITY_TYPE_IN_SAME_TABLE:
                    processDifferentEntityTypeInSameTable();
                    break;
            }
        }
        catch(Exception e)
        {
            log.error("discoveryRelationships() failed! e="+e);
            throw e;
        }
        
        return relationships;
    }
     
    private String getEntityTypeNodeDescription(String entityTypeDataobjectTypeNameField,JSONObject resultFields)
    {
        String description = "";
        
        if ( entityTypeDataobjectTypeNameField == null || entityTypeDataobjectTypeNameField.trim().isEmpty() )
            return description;
         
        description = resultFields.optString(entityTypeDataobjectTypeNameField);
        
        return description;
    }
    
    public void processSameEntityTypeSameAttributeSameValueRelationship() throws Exception
    {
        String filterStr = "";
        List<String> indexTypes = new ArrayList<>();
        boolean isDataFromAssociatedData;
        DataobjectVO associatedDataobjectVO = null;
        String val;
        String foundDataobjectId;
        DataobjectType associatedDataobjectType = null;
        String foundDataobjectName = "";
        String foundDataobjectDescription = "";
        List<String> foundDataobjectIdsLastTime = new ArrayList<>();
        List<String> foundDataobjectIdsThisTime = new ArrayList<>();
       
        ArrayList<Map<String,Object>> foundRelationships = new ArrayList<>();
        
        try
        {
            int catalogId = repository.getDefaultCatalogId();
     
            SearchRequest request = new SearchRequest(task.getOrganizationId(),task.getRepositoryId(),catalogId,SearchType.SEARCH_GET_ALL_RESULTS.getValue());

            int searchScope = dataobjectType.getIsEventData()==1?SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue():SearchScope.NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue();
        
            request.setSearchScope(searchScope);
             
            if ( columnMappingList.isEmpty() )
            {
                log.info(" column valus is empty, return");
                return;
            }
 
            for(int i=0;i<columnMappingList.size();i++)
            {
                filterStr = "";
                foundDataobjectIdsThisTime = new ArrayList<>();
                
                ColumnMapping columnMapping = columnMappingList.get(i);
                
                if ( columnMapping.getDataobjectTypeName().equals(dataobjectType.getName()) )
                {
                    isDataFromAssociatedData = false;
                }
                else
                {
                    isDataFromAssociatedData = true;
                    
                    DataobjectVO dataobjectVO = DataobjectHelper.getDataobjectDetails(em,platformEm,task.getOrganizationId(),dataobjectId,0,dataobjectType.getId(),repository.getId()); 
 
                    String associationTypes = "1"; // 1 to 1
                    List<Integer> associatedDataobjectTypeIds = new ArrayList<>();
                    
                    associatedDataobjectType = Util.getDataobjectTypeByName(platformEm, columnMapping.getDataobjectTypeName());
                    associatedDataobjectTypeIds.add(associatedDataobjectType.getId());
                    
                    DataobjectAssociatedData dataobjectAssociatedData = DataAssociationUtil.getSingleLevelAssociatedData(em,platformEm,task.getOrganizationId(), repository.getId(), AssociationTypeProperty.PROFILE_ASSOCIATION.getValue(), associationTypes, dataobjectType.getId(), associatedDataobjectTypeIds, dataobjectVO,null, null);

                    if ( dataobjectAssociatedData.getAssociatedDataList() == null || dataobjectAssociatedData.getAssociatedDataList().isEmpty() )
                        return;

                    Map<String,Object> data  = dataobjectAssociatedData.getAssociatedDataList().get(0);
                    List<DataobjectVO> dataobjectVOList = (List<DataobjectVO>)data.get("dataobjectVOList");
            
                    associatedDataobjectVO = dataobjectVOList.get(0);
                }
                 
                indexTypes.add(String.format("%s_type",columnMapping.getDataobjectTypeName()));
                
                request.setTargetIndexTypes(indexTypes);
       
                String[] vals = columnMapping.getColumnName().split("\\,");
                
                for(int j=0;j<vals.length;j++)
                {                
                    if ( j > 0 )
                    {
                        filterStr += " AND ";
                    }

                    if ( isDataFromAssociatedData )
                        val = associatedDataobjectVO.getThisDataobjectTypeMetadatas().get(vals[j]);
                    else
                        val = dataobjectJson.optString(vals[j]);
                 
                    if ( val == null || val.isEmpty() )
                    {
                        log.warn(columnMapping.getColumnName()+"is null or empty, return!");
                        return;
                    }

                    filterStr += String.format("%s:\"%s\"",vals[j],val);
                }
                
                request.setFilterStr(filterStr);
                request.setSearchFrom(0);

                //request.setSortColumns("last_stored_time,ASC");

                SearchResponse response = null;
                List<Map<String,String>> searchResults;
                String scrollId;
            
                int retry = 0;

                while(true)
                {
                    retry++;

                    response = SearchUtil.getDataobjectsWithScroll(em,platformEm,request,FETCH_SIZE,KEEP_LIVE_SECONDS);

                    searchResults = response.getSearchResults();
                    scrollId = response.getScrollId();

                    if ( !searchResults.isEmpty() )
                        break;

                    if ( retry > 5 )
                    {
                        log.error("SearchUtil.getDataobjectsWithScroll() get 0 result! exit! ");
                        break;
                    }

                    log.info(" SearchUtil.getDataobjectsWithScroll() get 0 result! retry="+retry);
                    Tool.SleepAWhile(1, 0);
                }

                while(true)
                {
                    // process dataobjectVOs
                    log.info(" 11111111111111111111 dataobjectVOs size="+searchResults.size());

                    int k = 0;

                    for(Map<String,String> searchResult : searchResults)
                    {    
                        k++;
                        log.info(" processing line = "+k);

                        JSONObject resultFields = new JSONObject(searchResult.get("fields"));

                        if ( isDataFromAssociatedData )
                        {
                            foundDataobjectId = getEntityDataobjectId(resultFields,null,dataobjectType.getId(),dataobjectType.getIndexTypeName(),associatedDataobjectType.getId());

                            if ( foundDataobjectId == null )
                            {
                                log.info(" entityDataobjectId not found! associatedDataobjectId="+associatedDataobjectVO.getId());
                                continue;
                            }
                                                                                        
                            if ( this.dataobjectId.equals(foundDataobjectId) )
                                continue;
                            
                            JSONObject foundEntityResultFields = ESUtil.getDocument(esClient,task.getOrganizationId(), foundDataobjectId, dataobjectType.getIndexTypeName());
            
                            foundDataobjectName = Util.getDataobjectTypePrimaryKeyValue(discoveryPrimaryKeys,foundEntityResultFields,null);   
                            foundDataobjectDescription = Util.getDataobjectTypeFieldValue(entityType.getDataobjectTypeNameFields(),foundEntityResultFields,null); 
                        }
                        else
                        {
                            foundDataobjectId = searchResult.get("id");
                            foundDataobjectId = foundDataobjectId.substring(0, 40);
                                                        
                            if ( this.dataobjectId.equals(foundDataobjectId) )
                                continue;
                                                        
                            foundDataobjectName = Util.getDataobjectTypePrimaryKeyValue(discoveryPrimaryKeys,resultFields,null);   
                            foundDataobjectDescription = Util.getDataobjectTypeFieldValue(entityType.getDataobjectTypeNameFields(),resultFields,null); 
                        }

                        Map<String,Object> relationshipMap = new HashMap<>();

                        relationshipMap.put("relationshipTypeShortName",relationshipType.getShortName());
                        relationshipMap.put("relationshipTypeName",relationshipType.getName());
                        relationshipMap.put("relationshipTypeFactor",relationshipType.getFactor());
                        relationshipMap.put("relationshipDescription",filterStr);
                        relationshipMap.put("foundDataobjectId",foundDataobjectId);
                        relationshipMap.put("foundDataobjectName",foundDataobjectName);
                        relationshipMap.put("foundDataobjectDescription",foundDataobjectDescription);
                         
                        foundRelationships.add(relationshipMap);
                        
                        foundDataobjectIdsThisTime.add(foundDataobjectId);
                    }

                    // get remaining data

                    if ( response.hasMoreData )
                    {
                        retry = 0;

                        while(true)
                        {
                            retry++;

                            response = SearchUtil.getRemainingDataobjects(em,platformEm,request.getRepositoryId(),scrollId,KEEP_LIVE_SECONDS);

                            log.info("1111111111111111 get search result! number ="+response.getSearchResults().size()+" hasMoreData="+response.hasMoreData);

                            if ( retry > 10 )
                            {
                                log.error("222222222222222222 failed to get remaining dataobject! retry="+retry);
                                break;
                            }

                            if ( response.getSearchResults().isEmpty() )
                            {
                                Tool.SleepAWhileInMS(100);
                                continue;
                            }

                            break;
                        }

                        searchResults = response.getSearchResults();
                    }
                    else
                        break;
                }
                                
                if ( i>0 )
                    foundDataobjectIdsLastTime.retainAll(foundDataobjectIdsThisTime);
                else
                    foundDataobjectIdsLastTime = foundDataobjectIdsThisTime;
            }
            
            if ( columnMappingList.size() == 1 )
                relationships = foundRelationships;
            else
            {
                for(String objId : foundDataobjectIdsLastTime)
                {
                    for(Map<String,Object> relationshipMap : foundRelationships)
                    {
                        String id = (String)relationshipMap.get("foundDataobjectId");
                     
                        if ( id.equals(objId) )
                        {
                            relationships.add(relationshipMap);
                            break;
                        }
                    }
                }
            }
        }
        catch(Exception e)
        {
            log.error("processSameEntityTypeSameAttributeSameValueRelationship() failed! e="+e);
            throw e;
        }           
    }
    
    public void processSameEntityTypeDifferentAttributeSameValueRelationship() throws Exception
    {
        boolean isDataFromAssociatedData;
        DataobjectType associatedDataobjectType = null;
        List<String> indexTypes;
        String filterStr;
        DataobjectVO associatedDataobjectVO;
        
        try
        {
            ColumnMapping columnMapping = columnMappingList.get(0);

            int catalogId = repository.getDefaultCatalogId();
     
            SearchRequest request = new SearchRequest(task.getOrganizationId(),task.getRepositoryId(),catalogId,SearchType.SEARCH_GET_ALL_RESULTS.getValue());

            int searchScope = dataobjectType.getIsEventData()==1?SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue():SearchScope.NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue();
        
            request.setSearchScope(searchScope); 
            indexTypes = Util.getAllTargetIndexType(platformEm,new Integer[]{dataobjectType.getId()});
            request.setTargetIndexTypes(indexTypes);
       
            if ( columnMapping.getDataobjectTypeName().equals(dataobjectType.getName()) )
            {
                isDataFromAssociatedData = false;
  
                String val = dataobjectJson.optString(columnMapping.getColumnName());

                if ( val == null || val.isEmpty() )
                {
                    log.warn(columnMapping.getColumnName()+"is null or empty, return!");
                    return;
                }      

                filterStr = String.format("%s:\"%s\"",columnMapping.getTargetColumnName(),val);
            }
            else
            {
                isDataFromAssociatedData = true;
                
                DataobjectVO dataobjectVO = DataobjectHelper.getDataobjectDetails(em,platformEm,task.getOrganizationId(),dataobjectId,0,dataobjectType.getId(),repository.getId()); 

                String associationTypes = "1"; // 1 to 1
                List<Integer> associatedDataobjectTypeIds = new ArrayList<>();

                associatedDataobjectType = Util.getDataobjectTypeByName(platformEm, columnMapping.getDataobjectTypeName());
                associatedDataobjectTypeIds.add(associatedDataobjectType.getId());

                DataobjectAssociatedData dataobjectAssociatedData = DataAssociationUtil.getSingleLevelAssociatedData(em,platformEm,task.getOrganizationId(), repository.getId(), AssociationTypeProperty.PROFILE_ASSOCIATION.getValue(), associationTypes, dataobjectType.getId(), associatedDataobjectTypeIds, dataobjectVO,null, null);

                if ( dataobjectAssociatedData.getAssociatedDataList() == null || dataobjectAssociatedData.getAssociatedDataList().isEmpty() )
                    return;

                Map<String,Object> data  = dataobjectAssociatedData.getAssociatedDataList().get(0);
                List<DataobjectVO> dataobjectVOList = (List<DataobjectVO>)data.get("dataobjectVOList");

                associatedDataobjectVO = dataobjectVOList.get(0);
                    
                String val = associatedDataobjectVO.getThisDataobjectTypeMetadatas().get(columnMapping.getColumnName());

                if ( val == null || val.isEmpty() )
                {
                    log.warn(columnMapping.getColumnName()+"is null or empty, return!");
                    return;
                }      

                filterStr = String.format("%s:\"%s\"",columnMapping.getTargetColumnName(),val);
            }            
                           
            request.setFilterStr(filterStr);
            request.setSearchFrom(0);
      
            //request.setSortColumns("last_stored_time,ASC");
 
            SearchResponse response = null;
            List<Map<String,String>> searchResults;
            String scrollId;
                  
            int retry = 0;

            while(true)
            {
                retry++;

                response = SearchUtil.getDataobjectsWithScroll(em,platformEm,request,FETCH_SIZE,KEEP_LIVE_SECONDS);
                
                searchResults = response.getSearchResults();
                scrollId = response.getScrollId();
              
                if ( !searchResults.isEmpty() )
                    break;

                if ( retry > 5 )
                {
                    log.error("SearchUtil.getDataobjectsWithScroll() get 0 result! exit! ");
                    break;
                }

                log.info(" SearchUtil.getDataobjectsWithScroll() get 0 result! retry="+retry);
                Tool.SleepAWhile(1, 0);
            }
                      
            while(true)
            {
                // process dataobjectVOs
                log.info(" 11111111111111111111 dataobjectVOs size="+searchResults.size());
                
                int i = 0;
                
                for(Map<String,String> searchResult : searchResults)
                {    
                    i++;
                    log.info(" processing line = "+i);
                    
                    JSONObject resultFields = new JSONObject(searchResult.get("fields"));
                    
                    String dataobjectId = searchResult.get("id");
                    dataobjectId = dataobjectId.substring(0, 40);
                    
                    if ( this.dataobjectId.equals(dataobjectId) )
                        continue;
                                        
                    Map<String,Object> relationshipMap = new HashMap<>();
            
                    relationshipMap.put("relationshipTypeShortName",relationshipType.getShortName());
                    relationshipMap.put("relationshipTypeName",relationshipType.getName());
                    relationshipMap.put("relationshipTypeFactor",relationshipType.getFactor());
                    relationshipMap.put("relationshipDescription",columnMapping.getColumnName()+":"+filterStr);
                    relationshipMap.put("foundDataobjectId",dataobjectId);
                                         
                    String foundDataobjectName = Util.getDataobjectTypePrimaryKeyValue(discoveryPrimaryKeys,resultFields,null);   
                    String foundDataobjectDescription = Util.getDataobjectTypeFieldValue(entityType.getDataobjectTypeNameFields(),resultFields,null); 
                       
                    relationshipMap.put("foundDataobjectName",foundDataobjectName);
                    relationshipMap.put("foundDataobjectDescription",foundDataobjectDescription);
                           
                    relationships.add(relationshipMap);
                }
     
                // get remaining data
                
                if ( response.hasMoreData )
                {
                    retry = 0;
                    
                    while(true)
                    {
                        retry++;
                        
                        response = SearchUtil.getRemainingDataobjects(em,platformEm,request.getRepositoryId(),scrollId,KEEP_LIVE_SECONDS);

                        log.info("1111111111111111 get search result! number ="+response.getSearchResults().size()+" hasMoreData="+response.hasMoreData);
                        
                        if ( retry > 10 )
                        {
                            log.error("222222222222222222 failed to get remaining dataobject! retry="+retry);
                            break;
                        }
                        
                        if ( response.getSearchResults().isEmpty() )
                        {
                            Tool.SleepAWhileInMS(100);
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
        catch(Exception e)
        {
            log.error("processSameEntityTypeSameAttributeSameValueRelationship() failed! e="+e);
            throw e;
        }           
    }
      
    public void processSameEntityTypeSameActionRelationship() throws Exception
    {
        Date relationshipTime = null;
        DataobjectVO dataobjectVO = null;
                
        ArrayList<Map<String,Object>> newRelationships = new ArrayList<>();
        Map<String,Integer> relationshipTimesMap = new HashMap<>();
                
        try
        {
            String associationTypes = "2"; // 1 to many
            List<Integer> entityActionDataobjectTypeIds = new ArrayList<>();
            entityActionDataobjectTypeIds.add(entityActionDataobjectType.getId());
            
            try
            {
                dataobjectVO = DataobjectHelper.getDataobjectDetails(em,platformEm,task.getOrganizationId(),dataobjectId,0,dataobjectType.getId(),repository.getId()); 
            }
            catch(Exception e)
            {
                log.error(" getDataobject details faiiled! e="+e);
                return;    
            }
            
            DataobjectAssociatedData dataobjectAssociatedData = DataAssociationUtil.getSingleLevelAssociatedData(em,platformEm,task.getOrganizationId(), repository.getId(), AssociationTypeProperty.EVENT_ASSOCIATION.getValue(), associationTypes, dataobjectType.getId(), entityActionDataobjectTypeIds, dataobjectVO,null, null);
 
            if ( dataobjectAssociatedData.getAssociatedDataList() == null || dataobjectAssociatedData.getAssociatedDataList().isEmpty() )
                return;
            
            Map<String,Object> data  = dataobjectAssociatedData.getAssociatedDataList().get(0);
            List<DataobjectVO> dataobjectVOList = (List<DataobjectVO>)data.get("dataobjectVOList");
            
            for(DataobjectVO actionData : dataobjectVOList)
            {  
                relationshipTime = null;
                int catalogId = repository.getDefaultCatalogId();

                SearchRequest request = new SearchRequest(task.getOrganizationId(),task.getRepositoryId(),catalogId,SearchType.SEARCH_GET_ALL_RESULTS.getValue());

                int searchScope = entityActionDataobjectType.getIsEventData()==1?SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue():SearchScope.NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue();

                request.setSearchScope(searchScope);

                List<String> indexTypes = Util.getAllTargetIndexType(platformEm,new Integer[]{entityActionDataobjectType.getId()});
                request.setTargetIndexTypes(indexTypes);

                String filterStr = ""; 

                for(int i=0;i<columnMappingList.size();i++)
                {
                    ColumnMapping columnMapping = columnMappingList.get(i);

                    if ( i> 0 )
                        filterStr += " AND ";

                    String val = actionData.getThisDataobjectTypeMetadatas().get(columnMapping.getColumnName());
                    
                    if ( val == null || val.isEmpty() )
                        //filterStr += String.format("(NOT (%s:*))",columnMapping.getTargetColumnName());
                    {
                        log.warn(columnMapping.getColumnName()+"is null or empty, return!");
                        return;
                    }
                    
                    if ( val.length() == 24 && val.endsWith("Z") && val.contains("T") && task.getTimeDifference()!=null && !task.getTimeDifference().isEmpty() )
                    {
                        int timeDiffInSeconds1 = 0;
                        int timeDiffInSeconds2 = 0;
                          
                        String[] vals = task.getTimeDifference().split("\\;");
                
                        for(int j=0;j<vals.length;j++)
                        {
                            String[] vals1 = vals[j].split("\\,");

                            if ( j==0 )
                                timeDiffInSeconds1 = Integer.parseInt(vals1[0])*3600+Integer.parseInt(vals1[1])*60+Integer.parseInt(vals1[2]);
                            else
                                timeDiffInSeconds2 = Integer.parseInt(vals1[0])*3600+Integer.parseInt(vals1[1])*60+Integer.parseInt(vals1[2]);
                        }
                        
                        Date date = Tool.convertESDateStringToDate(val);
                      
                        Date date1 = Tool.dateAddDiff(date, -1*timeDiffInSeconds1 , Calendar.SECOND);
                        Date date2 = Tool.dateAddDiff(date, timeDiffInSeconds2 , Calendar.SECOND);
                        String datetimeCondition = Tool.getESDateTimeCondition(columnMapping.getColumnName(),"ibt",date1,date2);
                       
                        filterStr += datetimeCondition;
                        
                        relationshipTime = date;
                    }
                    else
                        filterStr += String.format("%s:\"%s\"",columnMapping.getColumnName(),val);
                }

                request.setFilterStr(filterStr);
                request.setSearchFrom(0);

                //request.setSortColumns("last_stored_time,ASC");

                SearchResponse response = null;
                List<Map<String,String>> searchResults;
                String scrollId;
        
                int retry = 0;

                while(true)
                {
                    retry++;

                    response = SearchUtil.getDataobjectsWithScroll(em,platformEm,request,FETCH_SIZE,KEEP_LIVE_SECONDS);

                    searchResults = response.getSearchResults();
                    scrollId = response.getScrollId();

                    if ( !searchResults.isEmpty() )
                        break;

                    if ( retry > 5 )
                    {
                        log.error("SearchUtil.getDataobjectsWithScroll() get 0 result! exit! ");
                        break;
                    }

                    log.info(" SearchUtil.getDataobjectsWithScroll() get 0 result! retry="+retry);
                    Tool.SleepAWhile(1, 0);
                }

                while(true)
                {
                    // process dataobjectVOs
                    log.info(" 11111111111111111111 dataobjectVOs size="+searchResults.size());

                    int i = 0;

                    for(Map<String,String> searchResult : searchResults)
                    {    
                        i++;
                        log.info(" processing line = "+i);
    
                        JSONObject resultFields = new JSONObject(searchResult.get("fields"));

                        String actionDataobjectId = searchResult.get("id");
                        actionDataobjectId = actionDataobjectId.substring(0, 40);

                        if ( actionData.getId().equals(actionDataobjectId) )
                            continue;

                        String entityDataobjectId = getEntityDataobjectId(resultFields,null,dataobjectType.getId(),dataobjectType.getIndexTypeName(),entityActionDataobjectType.getId());
                        
                        if ( entityDataobjectId == null )
                        {
                            log.info(" entityDataobjectId not found! actionDataobjectId="+actionDataobjectId);
                            continue;
                        }
                        
                        Map<String,Object> relationshipMap = new HashMap<>();

                        relationshipMap.put("relationshipTypeShortName",relationshipType.getShortName());
                        relationshipMap.put("relationshipTypeName",relationshipType.getName());
                        relationshipMap.put("relationshipTypeFactor",relationshipType.getFactor());
                        relationshipMap.put("relationshipDescription",filterStr);
                        relationshipMap.put("foundDataobjectId",entityDataobjectId);
                        relationshipMap.put("relationshipTime",relationshipTime);
                        relationshipMap.put("relationshipTimes",1);

                        JSONObject foundEntityResultFields = ESUtil.getDocument(esClient,task.getOrganizationId(), entityDataobjectId, dataobjectType.getIndexTypeName());
            
                        String foundDataobjectName = getDataobjectTypePrimaryKeyValue1(discoveryPrimaryKeys,foundEntityResultFields,null);   
                        relationshipMap.put("foundDataobjectName",foundDataobjectName);

                        String foundDataobjectDescription = Util.getDataobjectTypeFieldValue(entityType.getDataobjectTypeNameFields(),foundEntityResultFields,null); 
                        relationshipMap.put("foundDataobjectDescription",foundDataobjectDescription);
                    
                        newRelationships.add(relationshipMap);
                        
                        if ( relationshipTimes > 1 )
                        {
                            Integer times = relationshipTimesMap.get(entityDataobjectId);
                        
                            if ( times == null )
                                relationshipTimesMap.put(entityDataobjectId, 1);
                            else
                                relationshipTimesMap.put(entityDataobjectId, times+1);
                        }
                    }

                    // get remaining data

                    if ( response.hasMoreData )
                    {
                        retry = 0;

                        while(true)
                        {
                            retry++;

                            response = SearchUtil.getRemainingDataobjects(em,platformEm,request.getRepositoryId(),scrollId,KEEP_LIVE_SECONDS);

                            log.info("1111111111111111 get search result! number ="+response.getSearchResults().size()+" hasMoreData="+response.hasMoreData);

                            if ( retry > 10 )
                            {
                                log.error("222222222222222222 failed to get remaining dataobject! retry="+retry);
                                break;
                            }

                            if ( response.getSearchResults().isEmpty() )
                            {
                                Tool.SleepAWhileInMS(100);
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
                                        
            if ( relationshipTimes > 1 )
            {
                for( Map.Entry<String,Integer> entry : relationshipTimesMap.entrySet() )
                {
                    Integer times = entry.getValue(); log.info(" times="+times+" relationshipTimes="+relationshipTimes);

                    if ( times >= relationshipTimes ) // add
                    {
                        String foundDataobjectTypeId = entry.getKey();

                        for(Map<String,Object> map : newRelationships)
                        {
                            String id = (String)map.get("foundDataobjectId");

                            if ( id.equals(foundDataobjectTypeId) )
                            {
                                map.put("relationshipTimes",times);
                                relationships.add(map);
                                break;
                            }
                        }
                    }
                }
            }
            else                
                relationships.addAll(newRelationships);
        }
        catch(Exception e)
        {
            log.error("processSameEntityTypeSameActionRelationship() failed! e="+e);
            throw e;
        }           
    }
    
    public void processSameEntityTypeSameActionRelatedRelationship() throws Exception
    {           
        try
        {
            String associationTypes = "2"; // 1 to many
            List<Integer> entityActionDataobjectTypeIds = new ArrayList<>();
            entityActionDataobjectTypeIds.add(entityActionDataobjectType.getId());
            DataobjectVO dataobjectVO = DataobjectHelper.getDataobjectDetails(em,platformEm,task.getOrganizationId(),dataobjectId,0,dataobjectType.getId(),repository.getId()); 
 
            DataobjectAssociatedData dataobjectAssociatedData = DataAssociationUtil.getSingleLevelAssociatedData(em,platformEm,task.getOrganizationId(), repository.getId(), AssociationTypeProperty.EVENT_ASSOCIATION.getValue(), associationTypes, dataobjectType.getId(), entityActionDataobjectTypeIds, dataobjectVO,null, null);
 
            if ( dataobjectAssociatedData.getAssociatedDataList() == null || dataobjectAssociatedData.getAssociatedDataList().isEmpty() )
            {
                log.warn("no associated event data,return!");
                return;
            }
                 
            Map<String,Object> data  = dataobjectAssociatedData.getAssociatedDataList().get(0);
            List<DataobjectVO> dataobjectVOList = (List<DataobjectVO>)data.get("dataobjectVOList");
            
            for(DataobjectVO actionData : dataobjectVOList)
            {             
                ColumnMapping columnMapping = columnMappingList.get(0);
                
                String relationshipDescription = String.format("[%s]%s:%s - %s:%s",entityActionDataobjectType.getName(),columnMapping.getColumnName(),actionData.getThisDataobjectTypeMetadatas().get(columnMapping.getColumnName()),
                        columnMapping.getTargetColumnName(),actionData.getThisDataobjectTypeMetadatas().get(columnMapping.getTargetColumnName()));
                
                actionData.getThisDataobjectTypeMetadatas().put(columnMapping.getColumnName(),actionData.getThisDataobjectTypeMetadatas().get(columnMapping.getTargetColumnName()));
                
                String entityDataobjectId = getEntityDataobjectId(null,actionData,dataobjectType.getId(),dataobjectType.getIndexTypeName(),entityActionDataobjectType.getId());
              
                if ( entityDataobjectId == null )
                {
                    log.info(" entityDataobjectId not found! relationshipDescription="+relationshipDescription);
                    continue;
                }
                
                Map<String,Object> relationshipMap = new HashMap<>();

                relationshipMap.put("relationshipTypeShortName",relationshipType.getShortName());
                relationshipMap.put("relationshipTypeName",relationshipType.getName());
                relationshipMap.put("relationshipTypeFactor",relationshipType.getFactor());
             
                relationshipMap.put("relationshipDescription",relationshipDescription);
                relationshipMap.put("foundDataobjectId",entityDataobjectId);

                if ( entityActionDataobjectType.getEventTimeMetadataName() != null && !entityActionDataobjectType.getEventTimeMetadataName().trim().isEmpty() )
                {
                    String relationshipTime = actionData.getThisDataobjectTypeMetadatas().get(entityActionDataobjectType.getEventTimeMetadataName().trim());
                    Date date = Tool.convertESDateStringToDate(relationshipTime);
                    relationshipMap.put("relationshipTime",date);
                }
                
                JSONObject foundEntityResultFields = ESUtil.getDocument(esClient,task.getOrganizationId(), entityDataobjectId, dataobjectType.getIndexTypeName());
            
                String foundDataobjectName = getDataobjectTypePrimaryKeyValue1(discoveryPrimaryKeys,foundEntityResultFields,null);    
                relationshipMap.put("foundDataobjectName",foundDataobjectName);
 
                String foundDataobjectDescription = Util.getDataobjectTypeFieldValue(discoveryEntityType.getDataobjectTypeNameFields(),foundEntityResultFields,null); 
                relationshipMap.put("foundDataobjectDescription",foundDataobjectDescription);
                    
                relationships.add(relationshipMap);
            }
        }
        catch(Exception e)
        {
            log.error("processSameEntityTypeSameActionRelatedRelationship() failed! e="+e);
            throw e;
        }           
    }
    
    public void processDifferentEntityTypeInSameTable() throws Exception
    {           
        List<String> indexTypes;
        String filterStr1,filterStr2;
        
        //2290,CORE_BCFMCMBI_CINOCSNO-客户内码,CORE_BDFMHQAB_AB05CSNO-客户内码,CORE_BDFMHQAB_AB01AC15-账号,CORE_BDFMHQAB_AB01AC15-账号
                
        try
        {      
            int catalogId = repository.getDefaultCatalogId();
     
            SearchRequest request = new SearchRequest(task.getOrganizationId(),task.getRepositoryId(),catalogId,SearchType.SEARCH_GET_ALL_RESULTS.getValue());

            int searchScope = relatedDataobjectType.getIsEventData()==1?SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue():SearchScope.NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue();
        
            request.setSearchScope(searchScope); 
            indexTypes = Util.getAllTargetIndexType(platformEm,new Integer[]{relatedDataobjectType.getId()});
            request.setTargetIndexTypes(indexTypes);
    
            filterStr1 = String.format("%s:\"%s\"",relatedDataobjectTypeFieldName1,dataobjectJson.optString(entityFieldName));
                                           
            request.setFilterStr(filterStr1);
            request.setSearchFrom(0);
 
            SearchResponse response = null;
            List<Map<String,String>> searchResults;
            String scrollId;
                  
            int retry = 0;

            while(true)
            {
                retry++;

                response = SearchUtil.getDataobjectsWithScroll(em,platformEm,request,FETCH_SIZE,KEEP_LIVE_SECONDS);
                
                searchResults = response.getSearchResults();
                scrollId = response.getScrollId();
              
                if ( !searchResults.isEmpty() )
                    break;

                if ( retry > 5 )
                {
                    log.error("SearchUtil.getDataobjectsWithScroll() get 0 result! exit! ");
                    break;
                }

                log.info(" SearchUtil.getDataobjectsWithScroll() get 0 result! retry="+retry);
                //Tool.SleepAWhile(1, 0);
            }
                      
            while(true)
            {
                // process dataobjectVOs
                log.info(" 11111111111111111111 dataobjectVOs size="+searchResults.size());
                
                int i = 0;
                
                for(Map<String,String> searchResult : searchResults)
                {    
                    i++;
                    log.info(" processing line = "+i);
               
                    JSONObject resultFields = new JSONObject(searchResult.get("fields"));
                
                    filterStr2 = String.format("%s:\"%s\"",discoveryEntityFiledName,resultFields.get(relatedDataobjectTypeFieldName2));
                   
                    Map<String,Object> ret = ESUtil.searchDocument(esClient, new String[]{}, new String[]{associatedDataobjectType.getIndexTypeName()}, new String[]{}, null, filterStr2, 1,0, new String[]{}, new String[]{});

                    List<Map<String,Object>> searchResults1 = (List<Map<String,Object>>)ret.get("searchResults");

                    if ( searchResults1 == null || searchResults1.isEmpty() )
                        continue;
                   
                    String foundDataobjectId = (String)searchResults1.get(0).get("id");
                    foundDataobjectId = foundDataobjectId.substring(0, 40);
                    
                    Map<String,Object> relationshipMap = new HashMap<>();
            
                    relationshipMap.put("relationshipTypeShortName",relationshipType.getShortName());
                    relationshipMap.put("relationshipTypeName",relationshipType.getName());
                    relationshipMap.put("relationshipTypeFactor",relationshipType.getFactor());
                    relationshipMap.put("relationshipDescription",filterStr1+","+filterStr2);
                    relationshipMap.put("foundDataobjectId",foundDataobjectId);
             
                    String foundDataobjectName = "";
                
                    JSONObject resultFields1 = new JSONObject((String)searchResults1.get(0).get("fields"));
                
                    for(String primaryKeysFieldName : discoveryPrimaryKeys)
                        foundDataobjectName += resultFields1.optString(primaryKeysFieldName) + "."; 
                    
                    if ( !foundDataobjectName.isEmpty() )
                        foundDataobjectName = foundDataobjectName.substring(0, foundDataobjectName.length()-1);
                    
                    relationshipMap.put("foundDataobjectName",foundDataobjectName);
                    
                    String foundDataobjectDescription = Util.getDataobjectTypeFieldValue(discoveryEntityType.getDataobjectTypeNameFields(),resultFields1,null);
                    relationshipMap.put("foundDataobjectDescription",foundDataobjectDescription);
                                
                    relationships.add(relationshipMap);
                }
     
                // get remaining data
                
                if ( response.hasMoreData )
                {
                    retry = 0;
                    
                    while(true)
                    {
                        retry++;
                        
                        response = SearchUtil.getRemainingDataobjects(em,platformEm,request.getRepositoryId(),scrollId,KEEP_LIVE_SECONDS);

                        log.info("1111111111111111 get search result! number ="+response.getSearchResults().size()+" hasMoreData="+response.hasMoreData);
                        
                        if ( retry > 10 )
                        {
                            log.error("222222222222222222 failed to get remaining dataobject! retry="+retry);
                            break;
                        }
                        
                        if ( response.getSearchResults().isEmpty() )
                        {
                            Tool.SleepAWhileInMS(100);
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
        catch(Exception e)
        {
            log.error("processDifferentEntityTypeInSameTable() failed! e="+e);
            throw e;
        }           
    }
    
    public void processDifferentEntityTypeDifferentAttributeRelationship() throws Exception
    {
        DataobjectVO dataobjectVO = null;
        
        try
        {
            String associationTypes = "";
            List<Integer> entityActionDataobjectTypeIds = new ArrayList<>();
            entityActionDataobjectTypeIds.add(associatedDataobjectType.getId());
            
            try {
                dataobjectVO = DataobjectHelper.getDataobjectDetails(em,platformEm,task.getOrganizationId(),dataobjectId,0,dataobjectType.getId(),repository.getId()); 
            }
            catch(Exception e)
            {
                log.warn("11111111111 dataobject not found! dataobjectid="+dataobjectId);
                return;
            }
            
            DataobjectAssociatedData dataobjectAssociatedData = DataAssociationUtil.getSingleLevelAssociatedData(em,platformEm,task.getOrganizationId(), repository.getId(), AssociationTypeProperty.ALL_ASSOCIATION.getValue(), associationTypes, dataobjectType.getId(), entityActionDataobjectTypeIds, dataobjectVO,null, null);
 
            if ( dataobjectAssociatedData.getAssociatedDataList() == null || dataobjectAssociatedData.getAssociatedDataList().isEmpty() )
            {
                log.warn("no associated event data,return!");
                return;
            }
                 
            Map<String,Object> data  = dataobjectAssociatedData.getAssociatedDataList().get(0);
            List<DataobjectVO> dataobjectVOList = (List<DataobjectVO>)data.get("dataobjectVOList");
            
            for(DataobjectVO associatedData : dataobjectVOList)
            {                   
                Map<String,Object> relationshipMap = new HashMap<>();

                relationshipMap.put("relationshipTypeShortName",relationshipType.getShortName());
                relationshipMap.put("relationshipTypeName",relationshipType.getName());
                relationshipMap.put("relationshipTypeFactor",relationshipType.getFactor());
                relationshipMap.put("foundDataobjectId",associatedData.getId());
  
                String foundDataobjectName = getDataobjectTypePrimaryKeyValue1(discoveryPrimaryKeys,null,associatedData);    
                relationshipMap.put("foundDataobjectName",foundDataobjectName);
                       
                String foundDataobjectDescription = Util.getDataobjectTypeFieldValue(discoveryEntityType.getDataobjectTypeNameFields(),null,associatedData);
                relationshipMap.put("foundDataobjectDescription",foundDataobjectDescription);
                        
                String relationshipDescription = String.format("dataobjectType:%s,associatedDataobjectType:%s,dataobjectName=%s",dataobjectType.getName(),associatedDataobjectType.getName(),foundDataobjectName);
                relationshipMap.put("relationshipDescription",relationshipDescription);                    

                relationships.add(relationshipMap);
            }
        }
        catch(Exception e)
        {
            log.error("processSameEntityTypeSameActionRelatedRelationship() failed! e="+e);
            throw e;
        }           
    }        
        
    private String getDataobjectTypePrimaryKeyValue1(List<String> primaryKeyFieldNames,JSONObject dataobjectJson,DataobjectVO dataobjectVO)
    {     
        String val = "";
        
        for(String primaryKeysFieldName : primaryKeyFieldNames)
        {
            //String slaveFieldName = DataAssociationUtil.getSlaveFieldName(platformEm,task.getOrganizationId(),dataobjectType.getId(),entityActionDataobjectType.getId(),primaryKeysFieldName);
            
            //if ( slaveFieldName == null )
            //    continue;
            
            if ( dataobjectJson != null )
                val += dataobjectJson.optString(primaryKeysFieldName) + ".";
            else
                val += dataobjectVO.getThisDataobjectTypeMetadatas().get(primaryKeysFieldName) + ".";
        }   
        if ( !val.isEmpty() )
            val = val.substring(0, val.length()-1);
        
        return val;
    }
    
    private String getDataobjectTypePrimaryKeyValue2(List<String> primaryKeyFieldNames,JSONObject dataobjectJson,DataobjectVO dataobjectVO)
    {     
        String val = "";
        
        for(String primaryKeysFieldName : primaryKeyFieldNames)
        {
            String slaveFieldName = DataAssociationUtil.getSlaveFieldName(platformEm,task.getOrganizationId(),dataobjectType.getId(),entityActionDataobjectType.getId(),primaryKeysFieldName);
            
            if ( slaveFieldName == null )
                continue;
            
            if ( dataobjectJson != null )
                val += dataobjectJson.optString(slaveFieldName) + ".";
            else
                val += dataobjectVO.getThisDataobjectTypeMetadatas().get(slaveFieldName) + ".";
        }   
        if ( !val.isEmpty() )
            val = val.substring(0, val.length()-1);
        
        return val;
    }
    
    private String getEntityDataobjectId(JSONObject dataobjectJson,DataobjectVO dataobjectVO,int masterDataobjectTypeId,String masterDataobjectTypeIndexName,int associatedDataobjectTypeId)
    {
        String queryStr = "";

        Map<String,String> masterSlaveFieldNameMap = DataAssociationUtil.getSlaveFieldNames(platformEm,task.getOrganizationId(),masterDataobjectTypeId,associatedDataobjectTypeId);
          
        int i = 0;
       
        for( Map.Entry<String,String> entry : masterSlaveFieldNameMap.entrySet() )
        {            
            if ( i > 0)
                queryStr += " AND ";
            
            if ( dataobjectJson != null)
                queryStr += String.format("%s:\"%s\"",entry.getKey(),dataobjectJson.optString(entry.getValue()));
            else
                queryStr += String.format("%s:\"%s\"",entry.getKey(),dataobjectVO.getThisDataobjectTypeMetadatas().get(entry.getValue()));
            
            i++;
        }
             
        Map<String,Object> ret = ESUtil.searchDocument(esClient, new String[]{}, new String[]{masterDataobjectTypeIndexName}, new String[]{}, queryStr, null, 1,0, new String[]{}, new String[]{});

        List<Map<String,Object>> searchResults  = (List<Map<String,Object>> )ret.get("searchResults");
        
        for(Map<String,Object> map : searchResults)
        {
            String id = (String)map.get("id");
            id = id.substring(0, 40);
            
            return id;
        }
        
        return null;
    }
    
    public RelationshipDiscoveryTask getTask() {
        return task;
    }

    public void setTask(RelationshipDiscoveryTask task) {
        this.task = task;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public String getDataobjectId() {
        return dataobjectId;
    }

    public EntityManager getEm() {
        return em;
    }

    public void setEm(EntityManager em) {
        this.em = em;
    }

    public EntityManager getPlatformEm() {
        return platformEm;
    }

    public void setPlatformEm(EntityManager platformEm) {
        this.platformEm = platformEm;
    }

    public RelationshipDiscoveryJob getJob() {
        return job;
    }

    public void setJob(RelationshipDiscoveryJob job) {
        this.job = job;
    }

    public String getDataobjectName() {
        return dataobjectName;
    }

    public String getDataobjectDescription() {
        return dataobjectDescription;
    }
}

class ColumnMapping
{
    private String dataobjectTypeName;
    private String columnName;
    private String targetDataobjectTypeName;
    private String targetColumnName;
    private int entityActionDataojbectTypeId;
    private int associatedDataojbectTypeId;

    public ColumnMapping() {
    }

    public ColumnMapping(String dataobjectTypeName, String columnName, String targetDataobjectTypeName, String targetColumnName) {
        this.dataobjectTypeName = dataobjectTypeName;
        this.columnName = columnName;
        this.targetDataobjectTypeName = targetDataobjectTypeName;
        this.targetColumnName = targetColumnName;
    }

    public String getDataobjectTypeName() {
        return dataobjectTypeName;
    }

    public void setDataobjectTypeName(String dataobjectTypeName) {
        this.dataobjectTypeName = dataobjectTypeName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getTargetDataobjectTypeName() {
        return targetDataobjectTypeName;
    }

    public void setTargetDataobjectTypeName(String targetDataobjectTypeName) {
        this.targetDataobjectTypeName = targetDataobjectTypeName;
    }

    public String getTargetColumnName() {
        return targetColumnName;
    }

    public void setTargetColumnName(String targetColumnName) {
        this.targetColumnName = targetColumnName;
    }

    public int getEntityActionDataojbectTypeId() {
        return entityActionDataojbectTypeId;
    }

    public void setEntityActionDataojbectTypeId(int entityActionDataojbectTypeId) {
        this.entityActionDataojbectTypeId = entityActionDataojbectTypeId;
    }

    public int getAssociatedDataojbectTypeId() {
        return associatedDataojbectTypeId;
    }

    public void setAssociatedDataojbectTypeId(int associatedDataojbectTypeId) {
        this.associatedDataojbectTypeId = associatedDataojbectTypeId;
    }

    
}