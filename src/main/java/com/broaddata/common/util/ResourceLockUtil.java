
/*
 * ResourceLockUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Date;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.TypeMissingException;
import org.json.JSONObject;
import com.broaddata.common.model.enumeration.LockResourceStatus;
import java.util.Calendar;
 
public class ResourceLockUtil 
{      
    static final Logger log = Logger.getLogger("ResourceLockUtil");      
    
    static final String resourceLockIndexTypeName = "resource_lock";
    
    public static void createResourceLock(Client configEsClient,int organizationId,int lockResourceType,String resourceId) throws Exception
    {
        JSONObject record = null;
        
        try
        {
            String indexName = String.format("edf_system_config_%d",organizationId);
            String recordId = String.format("%d_%d_%s",organizationId,lockResourceType,resourceId);
            
            try
            {
                record = ESUtil.findRecord(configEsClient,indexName,resourceLockIndexTypeName,recordId);
            }
            catch(Exception e)
            {
                log.error(" findRecord failed! e="+e);
            }
            
            if ( record != null )
            {
                log.info("already has resource lock data! lockresourcetype="+lockResourceType+" resourceId="+resourceId);
                return;
            }
                        
            Date currentTime = new Date(); 
         
            Map<String,Object> jsonMap = new HashMap<>();
 
            jsonMap.put("organization_id", organizationId);
            jsonMap.put("lock_resource_type",lockResourceType);
            jsonMap.put("resourceId", resourceId);
            jsonMap.put("status",LockResourceStatus.UN_LOCKED.getValue());
            jsonMap.put("status_time",currentTime);
            jsonMap.put("last_update_time",currentTime);
            jsonMap.put("locked_client_id", "");
            jsonMap.put("other_info","");
               
            while(true)
            {
                try
                {            
                    ESUtil.persistRecord(configEsClient, indexName, resourceLockIndexTypeName,recordId,jsonMap,0);
                    break;
                }
                catch(Exception ee)
                {
                    if ( ee instanceof TypeMissingException )
                    {
                        List<Map<String,String>> definition = Util.prepareResourceLockIndexTypeDefinition();
                        XContentBuilder typeMapping = ESUtil.createTypeMapping(resourceLockIndexTypeName,definition);
                        ESUtil.putMappingToIndex(configEsClient, indexName, resourceLockIndexTypeName, typeMapping);
                        continue;
                    }
     
                    throw ee;
                }                
            }
        }
        catch(Exception e)
        {
            log.error(" createResourceLock() failed! e="+e);
            throw e;
        }
    }
    
    public static long getResourceLock(Client configEsClient,int organizationId,int lockResourceType,String resourceId,String clientId,String otherInfo) throws Exception
    {
        JSONObject record = null;
        long oldVersion = 0;
        Map<String,Object> result;

        String indexName = String.format("edf_system_config_%d",organizationId);
        String recordId = String.format("%d_%d_%s",organizationId,lockResourceType,resourceId);
            
        try
        { 
            result = ESUtil.findRecordWithVersion(configEsClient,indexName,resourceLockIndexTypeName,recordId);
            
            if ( result == null )
            {
                record = null;
                oldVersion = 0;
            }
            else
            {
                record = (JSONObject)result.get("jsonObject");
                oldVersion = (long)result.get("version");
            }
        }
        catch(Exception e)
        {
            log.error(" findRecord failed! e="+e);
        }

        if ( record == null )
            return -1;
        else
        {
            int status = record.optInt("status");

            if ( status == LockResourceStatus.LOCKED.getValue() )
            {
                String dateStr = record.optString("last_update_time");
                Date date = Tool.convertESDateStringToDate(dateStr);
                Date newDate = Tool.dateAddDiff(new Date(), -3, Calendar.MINUTE);

                if ( newDate.before(date) )
                {
                    log.debug("2222222 status is locked, and last update time is less than 5 minutes! lasttupdatetime="+dateStr+" "+" status="+status);
                    return -2;
                }
                
                log.debug("2222222 found status is locked and expired! resourceId="+resourceId+" type="+lockResourceType);
            }
            else
                log.debug("333333 found status is unlocked! resourceid="+resourceId+" type="+lockResourceType); 

            // need to lock this record
            Date currentTime = new Date(); 

            Map<String,Object> jsonMap = new HashMap<>();
 
            jsonMap.put("organization_id", organizationId);
            jsonMap.put("lock_resource_type",lockResourceType);
            jsonMap.put("resourceId", resourceId);
            jsonMap.put("status",LockResourceStatus.LOCKED.getValue());
            jsonMap.put("status_time",currentTime);
            jsonMap.put("last_update_time",currentTime);
            jsonMap.put("locked_client_id", clientId);
            jsonMap.put("other_info",otherInfo);

            log.debug(" try to get lock!  oldVersion="+oldVersion);
  
            while(true)
            {
                try
                {            
                    oldVersion = ESUtil.persistRecord(configEsClient, indexName, resourceLockIndexTypeName, recordId, jsonMap,oldVersion);
                    break;
                }
                catch(Exception e)
                {
                    if ( e instanceof TypeMissingException )
                    {
                        List<Map<String,String>> definition = Util.prepareResourceLockIndexTypeDefinition();
                        XContentBuilder typeMapping = ESUtil.createTypeMapping(resourceLockIndexTypeName,definition);
                        ESUtil.putMappingToIndex(configEsClient, indexName, resourceLockIndexTypeName, typeMapping);
                        continue;
                    }
     
                    log.debug(" lock and update record failed! e="+e);
                    return -3;
                }                
            }
            
            log.info(" got the lock !!!!!!!!!!!!");
                    
            return oldVersion;
        }
    }
    
    public static long updateLockInfo(Client configEsClient,int organizationId,int lockResourceType,String resourceId,String clientId,String otherInfo) throws Exception
    {
        JSONObject record = null;
        long oldVersion = 0;

        String indexName = String.format("edf_system_config_%d",organizationId);
        String recordId = String.format("%d_%d_%s",organizationId,lockResourceType,resourceId);
            
        try
        {
            record = ESUtil.findRecord(configEsClient,indexName,resourceLockIndexTypeName,recordId);
        }
        catch(Exception e)
        {
            log.error(" findRecord failed! e="+e);
        }

        if ( record == null )
            return -1;
        else
        {
            int status = record.optInt("status");

            if ( status != LockResourceStatus.LOCKED.getValue() )
            {
                log.error(" wrong status!");
                return -2;
            }
            
            String newClientId = record.optString("locked_client_id");
            if ( !newClientId.equals(clientId) )
            {
                log.error(" not the right client id!");
                return -3;
            }

            // need to lock this record
            Date currentTime = new Date(); 

            Map<String,Object> jsonMap = new HashMap<>();
 
            jsonMap.put("organization_id", record.optInt("organization_id"));
            jsonMap.put("lock_resource_type",record.optInt("lock_resource_type"));
            jsonMap.put("resourceId", record.optString("resourceId"));
            jsonMap.put("status",record.optInt("status"));
            jsonMap.put("status_time",currentTime);
            jsonMap.put("last_update_time",currentTime);
            jsonMap.put("locked_client_id", clientId);
            jsonMap.put("other_info",otherInfo);

            log.debug(" try to update info!");
  
            while(true)
            {
                try
                {            
                    oldVersion = ESUtil.persistRecord(configEsClient, indexName, resourceLockIndexTypeName, recordId, jsonMap,0);
                    break;
                }
                catch(Exception e)
                {
                    if ( e instanceof TypeMissingException )
                    {
                        List<Map<String,String>> definition = Util.prepareResourceLockIndexTypeDefinition();
                        XContentBuilder typeMapping = ESUtil.createTypeMapping(resourceLockIndexTypeName,definition);
                        ESUtil.putMappingToIndex(configEsClient, indexName, resourceLockIndexTypeName, typeMapping);
                        continue;
                    }
     
                    log.info(" lock and update record failed! e="+e);
                    return -3;
                }                
            }
        
            return oldVersion;
        }
    }
    
    public static long releaseLock(Client configEsClient,int organizationId,int lockResourceType,String resourceId,String clientId,String otherInfo) throws Exception
    {
        JSONObject record = null;
        long oldVersion = 0;

        String indexName = String.format("edf_system_config_%d",organizationId);
        String recordId = String.format("%d_%d_%s",organizationId,lockResourceType,resourceId);
            
        try
        {
            record = ESUtil.findRecord(configEsClient,indexName,resourceLockIndexTypeName,recordId);
        }
        catch(Exception e)
        {
            log.error(" findRecord failed! e="+e);
        }

        if ( record == null )
            return -1;
        else
        {
            int status = record.optInt("status");

            if ( status != LockResourceStatus.LOCKED.getValue() )
            {
                log.error(" wrong status!");
                return -2;
            }
            
            String newClientId = record.optString("locked_client_id");
            if ( !newClientId.equals(clientId) )
            {
                log.error(" not the right client id!");
                return -3;
            }            
           
            // need to lock this record
            Date currentTime = new Date(); 

            Map<String,Object> jsonMap = new HashMap<>();
 
            jsonMap.put("organization_id", record.optInt("organization_id"));
            jsonMap.put("lock_resource_type",record.optInt("lock_resource_type"));
            jsonMap.put("resourceId", record.optString("resourceId"));
            jsonMap.put("status",LockResourceStatus.UN_LOCKED.getValue());
            jsonMap.put("status_time",currentTime);
            jsonMap.put("last_update_time",currentTime);
            jsonMap.put("locked_client_id", clientId);
            jsonMap.put("other_info",otherInfo);

            log.info(" try to update info!");
  
            while(true)
            {
                try
                {            
                    oldVersion = ESUtil.persistRecord(configEsClient, indexName, resourceLockIndexTypeName, recordId, jsonMap,0);
                    break;
                }
                catch(Exception e)
                {
                    if ( e instanceof TypeMissingException )
                    {
                        List<Map<String,String>> definition = Util.prepareResourceLockIndexTypeDefinition();
                        XContentBuilder typeMapping = ESUtil.createTypeMapping(resourceLockIndexTypeName,definition);
                        ESUtil.putMappingToIndex(configEsClient, indexName, resourceLockIndexTypeName, typeMapping);
                        continue;
                    }
     
                    log.info(" lock and update record failed! e="+e);
                    return -3;
                }                
            }
            
            log.info("relase lock successful !!!!!!!!!!!!");
                    
            return oldVersion;
        }
    }
}
