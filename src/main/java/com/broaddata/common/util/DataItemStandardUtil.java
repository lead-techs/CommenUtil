/*
 * DataItemStandardUtil.java 
 *
 */

package com.broaddata.common.util;
 
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManager;

import com.broaddata.common.model.organization.CodeDetails;
import com.broaddata.common.model.platform.DataItemStandard;
import com.broaddata.common.model.platform.DataobjectType;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.dom4j.Document;
import org.dom4j.Element;

public class DataItemStandardUtil
{      
    static final Logger log = Logger.getLogger("DataItemStandardUtil");      
 
    public static String getCodeValue(EntityManager em,int organizationId, int codeId, String codeKey)
    {
        CodeDetails codeDetails;
        
        try
        {
            String sql = String.format("from CodeDetails where organizationId=%d and codeId=%d and codeKey='%s'",organizationId,codeId,codeKey);
            codeDetails = (CodeDetails)em.createQuery(sql).getSingleResult();
            return codeDetails.getCodeValue();
        } 
        catch (Exception e) 
        {
            //log.error("getCodeValue() failed! e="+e);
            return "";
        }
    }
    
    public static List<Map<String,Object>> getDataItemStandardAssociations111(EntityManager platformEm,int organizationId, int dataItemStandardId) throws Exception
    {
        List<Map<String,Object>> dataobjectTypeInfo = new ArrayList<>();
        Map<String,Object> map;
        List<Map<String,Object>> metadtaDefintions;
        
        try
        {
            String sql = String.format("from DataobjectType where organizationId=%d and isSystemType=0", organizationId);
            List<DataobjectType> dataobjectTypeList = platformEm.createQuery(sql).getResultList();
 
            for(DataobjectType dataobjectType : dataobjectTypeList)
            {                
                metadtaDefintions = new ArrayList<>();
                Util.getSingleDataobjectTypeMetadataDefinition(metadtaDefintions,dataobjectType.getMetadatas());

                for(Map<String,Object> metadata : metadtaDefintions)
                { 
                    int currentDataItemStandardId = (Integer)metadata.get(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID);
                    String name = (String)metadata.get(CommonKeys.METADATA_NODE_NAME);
                    
                    if ( currentDataItemStandardId == dataItemStandardId )
                    {
                        map = new HashMap<>();
                        map.put("dataobjectType",dataobjectType);
                        map.put("sourceColumnName",name);
                        dataobjectTypeInfo.add(map);
                        break;
                    }
                }
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectTypesWithDataItemStandardId() failed!  e="+e);
            throw e;
        }
        
        return dataobjectTypeInfo;  
    }
    
    public static List<String> getDataobjectTypeWithDataItemStandardIdFieldNames(EntityManager platformEm,int organizationId, int dataobjectTypeId, int dataItemStandardId,int metadataGroup) throws Exception
    {
        List<Map<String,Object>> metadtaDefintions = new ArrayList<>();        
        List<String> fieldNames = new ArrayList<>();
        
        try
        {
            DataobjectType dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
            
            if ( dataobjectType == null )
            {
                log.warn((" dataobjectType not exists!  dataobjectTypeId="+dataobjectTypeId));
                return fieldNames;
            }
            
            Util.getSingleDataobjectTypeMetadataDefinition(metadtaDefintions,dataobjectType.getMetadatas());

            for(Map<String,Object> metadata : metadtaDefintions)
            { 
                int currentDataItemStandardId = (Integer)metadata.get(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID);
                int currentMetadataGroup = (Integer)metadata.get(CommonKeys.METADATA_NODE_METADATA_GROUP);
                
                if ( metadataGroup >= 0 )
                {
                    if ( currentMetadataGroup != metadataGroup )
                        continue;
                }
                
                String name = (String)metadata.get(CommonKeys.METADATA_NODE_NAME);
                          
                if ( currentDataItemStandardId == dataItemStandardId )
                    fieldNames.add(name);
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectTypeWithDataItemStandardIdFieldNames() failed!  e="+e+", stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
        
        return fieldNames;
    }
    
     
    public static List<String> getDataobjectTypeWithDataItemStandardIdFieldNames1(EntityManager platformEm,int organizationId, DataobjectType dataobjectType, int dataItemStandardId,int metadataGroup) throws Exception
    {
        List<Map<String,Object>> metadtaDefintions = new ArrayList<>();        
        List<String> fieldNames = new ArrayList<>();
        
        try
        {            
            Util.getSingleDataobjectTypeMetadataDefinition(metadtaDefintions,dataobjectType.getMetadatas());

            for(Map<String,Object> metadata : metadtaDefintions)
            { 
                int currentDataItemStandardId = (Integer)metadata.get(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID);
                int currentMetadataGroup = (Integer)metadata.get(CommonKeys.METADATA_NODE_METADATA_GROUP);
                
                if ( metadataGroup >= 0 )
                {
                    if ( currentMetadataGroup != metadataGroup )
                        continue;
                }
                
                String name = (String)metadata.get(CommonKeys.METADATA_NODE_NAME);
                          
                if ( currentDataItemStandardId == dataItemStandardId )
                    fieldNames.add(name);
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectTypeWithDataItemStandardIdFieldNames() failed!  e="+e+", stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
        
        return fieldNames;
    }
    
    public static float getDataobjectTypeHitFieldMaxFactor(DataobjectType dataobjectType, List<String> hitFieldNameList) throws Exception
    {
        float maxFactor = 1.0f;
        float factor;
        
        try
        {            
            if ( hitFieldNameList.isEmpty() )
                return maxFactor;
                    
            Document metadataXmlDoc = Tool.getXmlDocument(dataobjectType.getMetadatas());
            List<Element> nodes = metadataXmlDoc.selectNodes(CommonKeys.METADATA_NODE);

            for( Element node: nodes)
            {
                String fieldName = node.element(CommonKeys.METADATA_NODE_NAME).getTextTrim();
                
                boolean found = false;
                
                for(String name : hitFieldNameList)
                {
                    if ( name.trim().toLowerCase().equals(fieldName.toLowerCase()) )
                    {
                        found = true;
                        break;
                    }
                }
                
                if ( !found )
                    continue;
                        
                factor = node.element(CommonKeys.METADATA_NODE_SEARCH_FACTOR)!=null?Float.parseFloat(node.element(CommonKeys.METADATA_NODE_SEARCH_FACTOR).getTextTrim()):1.0f; // default 1.0 - not in search result
              
                if ( maxFactor < factor )
                    maxFactor = factor;
            }
        }
        catch(Exception e)
        {
            throw e;
        }
        
        return maxFactor;
    }
    
    public static Map<Integer,String> getDataobjectTypeDataItemStandardFieldNames(EntityManager platformEm,int organizationId, int dataobjectTypeId,boolean withDescription) throws Exception
    {
        List<Map<String,Object>> metadtaDefintions = new ArrayList<>();        
        Map<Integer,String> stdFieldNameMap = new HashMap<>();
        
        try
        {
            DataobjectType dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
            
            if ( dataobjectType == null )
            {
                log.warn((" dataobjectType not exists!  dataobjectTypeId="+dataobjectTypeId));
                return stdFieldNameMap;
            }
            
            Util.getSingleDataobjectTypeMetadataDefinition(metadtaDefintions,dataobjectType.getMetadatas());

            for(Map<String,Object> metadata : metadtaDefintions)
            { 
                int dataItemStandardId = (Integer)metadata.get(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID);
                
                if ( dataItemStandardId == 0 )
                    continue;
                
                String name = (String)metadata.get(CommonKeys.METADATA_NODE_NAME);
              
                if ( withDescription )
                {
                    String description = (String)metadata.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    name = String.format("%s-%s",name,description);
                }
                
                stdFieldNameMap.put(dataItemStandardId, name);
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectTypeDataItemStandardFieldNames() failed!  e="+e+", stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
        
        return stdFieldNameMap;
    }

    public static List<Map<String,Object>> getDataobjectTypesWithDataItemStandardId(EntityManager platformEm,int organizationId, int dataItemStandardId) throws Exception
    {
        List<Map<String,Object>> dataobjectTypeInfo = new ArrayList<>();
        Map<String,Object> map;
        List<Map<String,Object>> metadtaDefintions;
        
        try
        {
            String sql = String.format("from DataobjectType where organizationId=%d and isSystemType=0", organizationId);
            List<DataobjectType> dataobjectTypeList = platformEm.createQuery(sql).getResultList();
 
            for(DataobjectType dataobjectType : dataobjectTypeList)
            {                
                metadtaDefintions = new ArrayList<>();
                Util.getSingleDataobjectTypeMetadataDefinition(metadtaDefintions,dataobjectType.getMetadatas());

                for(Map<String,Object> metadata : metadtaDefintions)
                { 
                    int currentDataItemStandardId = (Integer)metadata.get(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID);
                    String name = (String)metadata.get(CommonKeys.METADATA_NODE_NAME);
                    String description = (String)metadata.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    String dataType = (String)metadata.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                    String length = (String)metadata.get(CommonKeys.METADATA_NODE_LENGTH);
                    String precision = (String)metadata.get(CommonKeys.METADATA_NODE_PRECISION);
                    int metadataGroup = (Integer)metadata.get(CommonKeys.METADATA_NODE_METADATA_GROUP);
                                                   
                    if ( currentDataItemStandardId == dataItemStandardId )
                    {
                        map = new HashMap<>();
                        
                        map.put("dataobjectType",dataobjectType);
                                         
                        map.put("dataobject_type_id",dataobjectType.getId());
                        map.put("dataobject_type_name",dataobjectType.getName());
                        map.put("dataobject_type_description",dataobjectType.getDescription());
                        map.put("metadata_name", name);
                        map.put("metadata_description", description);
                        map.put("metadata_data_type", dataType);
                        map.put("metadata_length", length.isEmpty()?0:Integer.parseInt(length));
                        map.put("metadata_precision", precision);            
                        map.put("data_item_standard_id",dataItemStandardId);
                        map.put("metadata_group",metadataGroup);
                        
                        dataobjectTypeInfo.add(map);
                        //break;
                    }
                }
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectTypesWithDataItemStandardId() failed!  e="+e);
            throw e;
        }
        
        return dataobjectTypeInfo;
    }    
    
    public static List<Map<String,Object>> getDataobjectTypesWithDataItemStandardId(EntityManager platformEm,int organizationId, int dataItemStandardId,boolean includeChildren) throws Exception
    {
        List<Map<String,Object>> dataobjectTypeInfo = new ArrayList<>();
        Map<String,Object> map;
        List<Map<String,Object>> metadtaDefintions;
        List<String> dataItemStandardIds = new ArrayList<>();
        
        try
        {
            if ( includeChildren )
            {
                List<DataItemStandard> list = getDataItemStandardsByIdIncludeChildren(platformEm,dataItemStandardId);    
                for(DataItemStandard std : list)
                    dataItemStandardIds.add(String.valueOf(std.getId()));
            }
            else
            {
                dataItemStandardIds.add(String.valueOf(dataItemStandardId));
            }
    
            String sql = String.format("from DataobjectType where organizationId=%d and isSystemType=0", organizationId);
            List<DataobjectType> dataobjectTypeList = platformEm.createQuery(sql).getResultList();
 
            for(DataobjectType dataobjectType : dataobjectTypeList)
            {                
                metadtaDefintions = new ArrayList<>();
                Util.getSingleDataobjectTypeMetadataDefinition(metadtaDefintions,dataobjectType.getMetadatas());

                for(Map<String,Object> metadata : metadtaDefintions)
                { 
                    int currentDataItemStandardId = (Integer)metadata.get(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID);
                    String name = (String)metadata.get(CommonKeys.METADATA_NODE_NAME);
                    String description = (String)metadata.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    String dataType = (String)metadata.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                    String length = (String)metadata.get(CommonKeys.METADATA_NODE_LENGTH);
                    String precision = (String)metadata.get(CommonKeys.METADATA_NODE_PRECISION);
                    int metadataGroup = (Integer)metadata.get(CommonKeys.METADATA_NODE_METADATA_GROUP);
                                                   
                    //if ( currentDataItemStandardId == dataItemStandardId )
                    String str = String.valueOf(currentDataItemStandardId);
                    
                    if ( dataItemStandardIds.contains(str) )
                    {
                        map = new HashMap<>();
                        
                        map.put("dataobjectType",dataobjectType);
                                         
                        map.put("dataobject_type_id",dataobjectType.getId());
                        map.put("dataobject_type_name",dataobjectType.getName());
                        map.put("dataobject_type_description",dataobjectType.getDescription());
                        map.put("metadata_name", name);
                        map.put("metadata_description", description);
                        map.put("metadata_data_type", dataType);
                        map.put("metadata_length", length.isEmpty()?0:Integer.parseInt(length));
                        map.put("metadata_precision", precision);            
                        
                        map.put("data_item_standard_id",currentDataItemStandardId);
                        map.put("parent_data_item_standard_id",dataItemStandardId);
                        
                        try
                        {
                            DataItemStandard std = platformEm.find(DataItemStandard.class, currentDataItemStandardId);
                            map.put("data_item_standard_name",std.getName());
                        }
                        catch(Exception e)
                        {
                            log.error(" data item standard not found! id="+currentDataItemStandardId);
                            map.put("data_item_standard_name","");
                        }
                        
                        map.put("metadata_group",metadataGroup);
                     
                        dataobjectTypeInfo.add(map);
                        //break;
                    }
                }
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectTypesWithDataItemStandardId() failed!  e="+e);
            throw e;
        }
        
        return dataobjectTypeInfo;
    }    
    
    private static boolean inDataItemStandardList(int dataItemStandardId,List<DataItemStandard> dataItemStandardList)
    {
        boolean found = false;
        
        for(DataItemStandard std : dataItemStandardList)
        {
            if ( std.getId() == dataItemStandardId )
            {
                found = true;
                break;
            }
        }
        
        return found;
    }
    
    private static boolean inMetadataGroup(String oldMetadataGroup,String newMetadataGroup) 
    {
        String[] vals = oldMetadataGroup.split("\\;");
        
        for(String val : vals)
        {
            if ( val.equals(newMetadataGroup) )
                return true;
        }
        
        return false;
    }
            
    public static List<Map<String,Object>> getDataobjectTypesWithDataItemStandardId1(EntityManager platformEm,int organizationId, List<DataItemStandard> dataItemStandardList) throws Exception
    {
        List<Map<String,Object>> dataobjectTypeInfo = new ArrayList<>();
        Map<String,Object> map;
        Map<String,String> groupMap = new HashMap<>();
        List<Map<String,Object>> metadtaDefintions;

        try
        {
            String sql = String.format("from DataobjectType where organizationId=%d and isSystemType=0", organizationId);
            List<DataobjectType> dataobjectTypeList = platformEm.createQuery(sql).getResultList();
 
            for(DataobjectType dataobjectType : dataobjectTypeList)
            {                
                metadtaDefintions = new ArrayList<>();
                Util.getSingleDataobjectTypeMetadataDefinition(metadtaDefintions,dataobjectType.getMetadatas());

                for(Map<String,Object> metadata : metadtaDefintions)
                { 
                    int currentDataItemStandardId = (Integer)metadata.get(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID);
                    String name = (String)metadata.get(CommonKeys.METADATA_NODE_NAME);
     
                    String metadataGroupStr = String.valueOf((Integer)metadata.get(CommonKeys.METADATA_NODE_METADATA_GROUP));
                    
                    if ( inDataItemStandardList(currentDataItemStandardId,dataItemStandardList) )
                    {
                        boolean found = false;
                        
                        for(Map<String,Object> newMap : dataobjectTypeInfo)
                        {
                            int cDataobjectTypeId = (Integer)newMap.get("dataobject_type_id");
                            Map<String,String> cGgroupMap = (Map<String,String>)newMap.get("metadata_group");
         
                            if ( cDataobjectTypeId == dataobjectType.getId() )
                            {
                                found = true;
                                                    
                                String cName = (String)cGgroupMap.get(metadataGroupStr);
                                
                                if ( cName == null )
                                    cGgroupMap.put(metadataGroupStr,name);
                                else
                                    cGgroupMap.put(metadataGroupStr,cName+"-"+name);
                             
                                break;
                            }
                        }
                        
                        if ( !found )
                        {                            
                            map = new HashMap<>();

                            map.put("dataobjectType",dataobjectType);
                            map.put("dataobject_type_id",dataobjectType.getId());
                            
                            groupMap = new HashMap<>();
                            groupMap.put(metadataGroupStr, name);
                                                     
                            map.put("metadata_group",groupMap);

                            dataobjectTypeInfo.add(map);
                        }
                                                
                        //break;
                    }
                }
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectTypesWithDataItemStandardId() failed!  e="+e);
            throw e;
        }
        
        return dataobjectTypeInfo;
    }    
    
    public static DataItemStandard getDataItemStandardByName(EntityManager platformEm,String dataItemStandardName) throws Exception 
    {
        DataItemStandard dataItemStandard = null;
        
        try 
        {
            String sql = String.format("from DataItemStandard where name='%s'",dataItemStandardName.trim());
            dataItemStandard = (DataItemStandard)platformEm.createQuery(sql).getSingleResult();
        } 
        catch (Exception e) {
             throw e;
        }
        
        return dataItemStandard;
    }
        
    public static List<DataItemStandard> getDataItemStandardByNamesIncludeChildren(EntityManager platformEm,String dataItemStandardName) throws Exception 
    {
        List<DataItemStandard> dataItemStandardList = new ArrayList<>();
        DataItemStandard dataItemStandard;
        
        try
        {
            String sql = String.format("from DataItemStandard where name='%s'",dataItemStandardName.trim());
            dataItemStandard = (DataItemStandard)platformEm.createQuery(sql).getSingleResult();
            
            sql = String.format("from DataItemStandard where parentId=%d",dataItemStandard.getId());
            
            dataItemStandardList = (List<DataItemStandard>)platformEm.createQuery(sql).getResultList();
            
            dataItemStandardList.add(dataItemStandard);
        }
        catch (Exception e) {
             throw e;
        }
        
        return dataItemStandardList;
    }
    
    public static List<DataItemStandard> getDataItemStandardsByIdIncludeChildren(EntityManager platformEm,int dataItemStandardId) throws Exception 
    {
        List<DataItemStandard> dataItemStandardList = new ArrayList<>();
        DataItemStandard dataItemStandard;
        
        try
        {
            String sql = String.format("from DataItemStandard where id=%d",dataItemStandardId);
            dataItemStandard = (DataItemStandard)platformEm.createQuery(sql).getSingleResult();
            
            sql = String.format("from DataItemStandard where parentId=%d",dataItemStandard.getId());
            
            dataItemStandardList = (List<DataItemStandard>)platformEm.createQuery(sql).getResultList();
            
            dataItemStandardList.add(dataItemStandard);
        }
        catch (Exception e) {
             throw e;
        }
        
        return dataItemStandardList;
    }    
    
    public static DataItemStandard getDataItemStandardById(EntityManager platformEm,int dataItemStandardId) throws Exception 
    {
        DataItemStandard dataItemStandard = null;
        
        try 
        {
            String sql = String.format("from DataItemStandard where id=%d",dataItemStandardId);
            dataItemStandard = (DataItemStandard)platformEm.createQuery(sql).getSingleResult();
        } 
        catch (Exception e) {
            log.error(" dataItemStandardId not found! dataItemStandardId="+dataItemStandardId);
            throw e;
        }
        
        return dataItemStandard;
    }
    
   
}
