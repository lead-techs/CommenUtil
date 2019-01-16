/*
 * TaggingUtil.java
 */

package com.broaddata.common.util;

import com.broaddata.common.model.organization.TagProcessingTask;
import com.broaddata.common.model.platform.TagDefinition;
import com.broaddata.common.model.platform.TagValue;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.elasticsearch.client.Client;
 
public class TaggingUtil
{      
    static final Logger log = Logger.getLogger("TaggingUtil");     
 
    public static List<String> getTagNames(EntityManager platformEm,int organizationId,int tagCategoryId)
    {
        String sql;
        List<String> list = new ArrayList<>();
    
        if ( tagCategoryId == 0 )
            sql = String.format("from TagDefinition where organizationId=%d order by name",organizationId);
        else
            sql = String.format("from TagDefinition where organizationId=%d and category=%d order by name",organizationId,tagCategoryId);

        List<TagDefinition> tagDefinitionList = platformEm.createQuery(sql).getResultList();
        
        for(TagDefinition tagDefinition : tagDefinitionList)
        {
            String tagName = String.format("%s-%s",tagDefinition.getShortName(),tagDefinition.getName());
            list.add(tagName);
        }
        
        return list;
    }
    
    public static List<String> getTagValues(EntityManager platformEm,int organizationId,int tagId)
    {
        String sql;
        List<String> list = new ArrayList<>();
    
        sql = String.format("from TagValue where organizationId=%d and tagDefinitionId=%d order by value",organizationId,tagId);
      
        List<TagValue> tagValueList = platformEm.createQuery(sql).getResultList();
        
        for(TagValue tagValue : tagValueList)
        {
            String tagName = String.format("%s-%s",tagValue.getValue(),tagValue.getDescription());
            list.add(tagName);
        }
        
        return list;
    }

    public static void addTagToDataobjects(Client esClient, List<String> dataobjectIds, String tagDefinitionShortName, String tagValue, String tagValueDescription, String manualInputedTag) throws Exception 
    {
        boolean found;
          
        try
        {
            for(String dataobjectId : dataobjectIds)
            {
                String queryStr = String.format("_id:%s",dataobjectId);
                
                Map<String,Object> searchRet = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, new String[]{},
                        queryStr, "", 1, 0, new String[]{"tags"}, new String[]{}, new String[]{},null);
                
                int currentHits = (Integer) searchRet.get("currentHits");
                
                if (currentHits == 0 ) 
                {
                    String errorInfo = "dataobject in es not found ! id="+ dataobjectId+" currentHits="+currentHits;
                    log.error(errorInfo);
                    continue;
                }
                 
                if (currentHits > 1) 
                {
                    String errorInfo = "dataobject in es more than 1 ! id="+ dataobjectId+" currentHits="+currentHits;
                    log.error(errorInfo);
                    
                    throw new Exception(errorInfo);
                }
                
                List<Map<String, Object>> searchResults = (List<Map<String, Object>>) searchRet.get("searchResults");
              
                for (Map<String, Object> result : searchResults) 
                {
                    String indexName = (String) result.get("index");
                    String indexType = (String)result.get("indexType");
                    
                    Map<String,Object> fieldMap = (Map<String,Object>)result.get("fields");
                    
                    Object obj = fieldMap.get("tags");
                    List<String> tags = new ArrayList<>();

                    if ( obj != null )
                    {
                        if ( obj instanceof Collection )
                            tags = (List<String>)obj;
                        else
                            tags.add((String)obj);
                    }
                    
                    fieldMap = new HashMap<>();
                    
                    List<String> newTags = new ArrayList<>();
                     
                    if ( tagDefinitionShortName==null || tagDefinitionShortName.trim().isEmpty() || tagValue==null || tagValue.trim().isEmpty() )
                    {
                        newTags = tags;    
                    }
                    else
                    {
                        String newTag = String.format("%s~%s",tagDefinitionShortName,tagValue);

                        found = false;
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
                    }
                                                            
                    if ( tagValueDescription != null && !tagValueDescription.trim().isEmpty() )
                    {
                        found = false;
                        for(String tag : newTags)
                        {
                            if ( tag.equals(tagValueDescription) )
                            {
                                found = true;
                                break;
                            }
                        }
                        
                        if ( !found )
                            newTags.add(tagValueDescription.trim());
                    }
                       
                    if ( manualInputedTag != null && !manualInputedTag.trim().isEmpty() )
                    {
                        found = false;
                        for(String tag : newTags)
                        {
                            if ( tag.equals(manualInputedTag) )
                            {
                                found = true;
                                break;
                            }
                        }
                        
                        if ( !found )
                            newTags.add(manualInputedTag.trim());
                    }                    
                    
                    fieldMap.put("tags",newTags);

                    esClient.prepareUpdate().setIndex(indexName).setType(indexType).setId(dataobjectId).setDoc(fieldMap).get();
                }
            }
        }
        catch(Exception e)
        {
            log.error("addTagToDataobjects() failed! e="+e);
            throw e;
        }
    }

    public static void removeTagFromDataobjects(EntityManager em, EntityManager platformEm, Client esClient,int organizationId, List<String> dataobjectIds, String tagDefinitionShortName) throws Exception
    {
        try
        {
            for(String dataobjectId : dataobjectIds)
            {
                String queryStr = String.format("_id:%s",dataobjectId);
                
                Map<String,Object> searchRet = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, new String[]{},
                        queryStr, "", 1, 0, new String[]{"tags"}, new String[]{}, new String[]{},null);
                
                int currentHits = (Integer) searchRet.get("currentHits");
                
                if (currentHits != 1) 
                {
                    String errorInfo = "dataobject in es not found or more than 1 ! id="+ dataobjectId+" currentHits="+currentHits;
                    log.error(errorInfo);
                    
                    throw new Exception(errorInfo);
                }
                
                List<Map<String, Object>> searchResults = (List<Map<String, Object>>) searchRet.get("searchResults");
              
                for (Map<String, Object> result : searchResults) 
                {
                    String indexName = (String) result.get("index");
                    String indexType = (String)result.get("indexType");
                    
                    Map<String,Object> fieldMap = (Map<String,Object>)result.get("fields");
                    
                    Object obj = fieldMap.get("tags");
                    List<String> tags = new ArrayList<>();

                    if ( obj != null )
                    {
                        if ( obj instanceof Collection )
                            tags = (List<String>)obj;
                        else
                            tags.add((String)obj);
                    }
                    
                    fieldMap = new HashMap<>();
                    
                    if ( tagDefinitionShortName == null || tagDefinitionShortName.trim().isEmpty() )
                    {
                         fieldMap.put("tags",new ArrayList<>());
                    }
                    else
                    {
                        List<String> newTags = new ArrayList<>();
 
                        for(String tag : tags)
                        {
                            if ( !tag.startsWith(tagDefinitionShortName.trim()) )
                                newTags.add(tag);
                        }                    
    
                        fieldMap.put("tags",newTags);
                    }

                    esClient.prepareUpdate().setIndex(indexName).setType(indexType).setId(dataobjectId).setDoc(fieldMap).get();
                }
            }
        }
        catch(Exception e)
        {
            log.error("removeTagFromDataobjects() failed! e="+e);
            throw e;
        } 
    }
 
    public static List<String> getTagsFromDataobject(EntityManager em, EntityManager platformEm, Client esClient,int organizationId, String dataobjectId) throws Exception 
    {
        List<String> tags = new ArrayList<>();
        
        try
        { 
            String queryStr = String.format("_id:%s",dataobjectId);

            Map<String,Object> searchRet = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, new String[]{},
                    queryStr, "", 1, 0, new String[]{"tags"}, new String[]{}, new String[]{},null);

            int currentHits = (Integer) searchRet.get("currentHits");

            if (currentHits != 1) 
            {
                String errorInfo = "dataobject in es not found or more than 1 ! id="+ dataobjectId+" currentHits="+currentHits;
                log.error(errorInfo);

                throw new Exception(errorInfo);
            }

            List<Map<String, Object>> searchResults = (List<Map<String, Object>>) searchRet.get("searchResults");

            for (Map<String, Object> result : searchResults) 
            {
                Map<String,Object> fieldMap = (Map<String,Object>)result.get("fields");

                Object obj = fieldMap.get("tags");
                tags = new ArrayList<>();

                if ( obj != null )
                {
                    if ( obj instanceof Collection )
                        tags = (List<String>)obj;
                    else
                        tags.add((String)obj);
                }
            }
        }
        catch(Exception e)
        {
            log.error("addTagToDataobjects() failed! e="+e);
            throw e;
        }
          
        return tags;
    }
        
    public static List<TagDefinition> getTagProcessingTaskTagDefinitions(EntityManager platformEm,TagProcessingTask task) 
    {
        String targetTagStr = "";
        String[] vals = task.getTargetTags().split("\\,");
        
        for(String val : vals)
            targetTagStr += val.substring(0, val.indexOf("-"))+",";
        
        targetTagStr = targetTagStr.substring(0, targetTagStr.length()-1);
        
        String sql = String.format("from TagDefinition where organizationId=%d and id in ( %s )",task.getOrganizationId(),targetTagStr);
        List<TagDefinition> tagDefinitionList = platformEm.createQuery(sql).getResultList();
        
        return tagDefinitionList;
    }
    
}
