/*
 * Processor.java
 *
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
import com.broaddata.common.model.enumeration.DatasourceEndJobType;
import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.DatasourceEndTask;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataProcessingExtension;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.util.CommonKeys;
import com.broaddata.common.util.CommonServiceConnector;
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.DataProcessingExtensionBase;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;

public abstract class Processor
{
    static final Logger log = Logger.getLogger("Processor");    
    protected DataService.Client dataService = null;
    protected CommonService.Client commonService = null;   
    protected DataServiceConnector dsConn = null;
    protected CommonServiceConnector csConn = null;
    protected Job job = null;
    protected DatasourceEndTask task = null;
    protected DatasourceType datasourceType = null;
    protected DataobjectType dataobjectType;
    protected String eventMetadataName;
    protected Date eventMetadataValue;
    protected DatasourceConnection datasourceConnection = null;    
    protected Datasource datasource = null;
    protected ComputingNode computingNode = null;
    protected DataProcessingExtensionBase dataProcessingExtension;
    protected ServiceInstance objectStorageServiceInstance = null;
    protected DataProcessingExtension dpe;
    protected int dataProcessingExtensionId;
    protected int dataValidationBehaviorTypeId = 1;
    protected int targetedFileEventTypeId = 1;
    protected Counter serviceCounter;
    
    public abstract boolean validateDataobject(DiscoveredDataobjectInfo obj);
    protected abstract void discoverDataobject() throws Exception;
    protected abstract void archiveDataobjects() throws Exception;
    
    protected abstract void getConfig(DataService.Client dataService,Datasource datasource,Job job) throws Exception;
    protected abstract void collectDataobjectMetadata(DataobjectInfo dataobject,String archiveFile) throws Exception;
    protected abstract void collectDataobjectContentMetadata(DataService.Client dataService,Datasource datasource,Job job,DataobjectInfo dataobject,List<String> contentIds, List<ByteBuffer> contentBinaries, List<String> contentLocations, List<Boolean> needToExtractContentTexts,String archiveFile) throws Exception;
    protected abstract void beforeStoringToServer(DataobjectInfo dataobject) throws Exception;
    protected abstract int getDataobjectTypeId(DataobjectInfo obj);
    public abstract void generateIndexMetadata(List<Map<String,Object>> metadataDefinitions,Map<String, Object> jsonMap, List<FrontendMetadata> metadataList, Dataobject obj, Map<String,Object> contentInfo,boolean needEncryption) throws Exception;
 
    public void executeJob(Counter serviceCounter) throws Exception
    {
        switch(DatasourceEndJobType.findByValue(job.jobType)) 
        {
            case DISCOVER:
                discoverDataobject();
                break;
            case ARCHIVE:
                archiveDataobjects();
                break;
            //case DELETE:
                //deleteDataobject();
            //    break;
            //case RESTORE:
                //restoreDataobject();
            //    break;
        }
    }
       
    protected void generateIndexMetadataDefault(List<Map<String,Object>> metadataDefinitions,Map<String, Object> jsonMap, List<FrontendMetadata> metadataList, Dataobject obj,Map<String,Object> contentInfo,boolean needEncryption) throws Exception 
    {
        String value; 
        
        int found = 0;
        
        for(Map<String,Object> map:metadataDefinitions)
        {               
            String name = (String)map.get(CommonKeys.METADATA_NODE_NAME);
            int dataType = Integer.parseInt((String)map.get(CommonKeys.METADATA_NODE_DATA_TYPE));
           
            value = Util.findSingleValueMetadataValue(name,metadataList);
            
            if ( value == null )
                value = "";
            
            //if ( value == null || value.trim().isEmpty() )
            //    continue;
            
            found ++;
            
            switch(MetadataDataType.findByValue(dataType))
            {
                case STRING:
                case BINARYLINK:  // file link 
                    //if ( needEncryption )
                    //    value = SecurityUtil.processEncyption(value,CommonKeys.ENCRYPTION_KEY);

                    jsonMap.put(name,value);
                    break;
                case BOOLEAN:
                    jsonMap.put(name,value.equals("true"));
                    break;
                case INTEGER:
                    try 
                    {
                        value = Tool.formalizeIntLongString(value);
                        jsonMap.put(name,Integer.parseInt(value));
                    }
                    catch(Exception e)
                    {
                        if ( value.trim().isEmpty() || value.trim().equals("null") )
                            jsonMap.put(name,Integer.parseInt("0"));
                        else
                        {
                            log.warn("generate integer error! e="+e+" name="+name+" value="+value+" dataType="+MetadataDataType.findByValue(dataType));
                            throw e;   
                        }
                    }
                    break;
                case LONG:
                    try 
                    {
                        value = Tool.formalizeIntLongString(value);
                        jsonMap.put(name,Long.parseLong(value));
                    }
                    catch(Exception e)
                    {
                        if ( value.trim().isEmpty() || value.trim().equals("null") )
                            jsonMap.put(name,Long.parseLong("0"));
                        else
                        {
                            log.warn("generate long error! e="+e+" name="+name+" value="+value+" dataType="+MetadataDataType.findByValue(dataType));
                            throw e;   
                        }
                        //log.warn("generateIndexMetadataDefault() error! e="+e+" name="+name+" value="+value+" dataType="+MetadataDataType.findByValue(dataType));
                    }                    
                    break;
                case FLOAT:
                    try 
                    {
                        jsonMap.put(name,Float.parseFloat(value));
                    }
                    catch(Exception e)
                    {
                        if ( value.trim().isEmpty() || value.trim().equals("null") )
                            jsonMap.put(name,Float.parseFloat("0.0"));
                        else
                        {
                            log.warn("generate float error! e="+e+" name="+name+" value="+value+" dataType="+MetadataDataType.findByValue(dataType));
                            throw e;   
                        }
                    }                        
                    break;
                case DOUBLE:
                    try 
                    {
                        jsonMap.put(name,Double.parseDouble(value));
                    }
                    catch(Exception e)
                    {
                        if ( value.trim().isEmpty() || value.trim().equals("null") )
                            jsonMap.put(name,Double.parseDouble("0.0"));
                        else
                        {
                            log.warn("generate double error! e="+e+" name="+name+" value="+value+" dataType="+MetadataDataType.findByValue(dataType));
                            throw e;   
                        }      
                    }                          
                    break;
                case TIMESTAMP:                    
                case DATE:
                    try 
                    {
                        jsonMap.put(name, new Date(Long.parseLong(value)));
                    }
                    catch(Exception e)
                    {
                        log.warn("generateIndexMetadataDefault() error! e="+e+" name="+name+" value="+value+" dataType="+MetadataDataType.findByValue(dataType));
                        if ( value.trim().isEmpty() )
                            jsonMap.put(name, new Date(Long.parseLong("0")));
                        else
                        {
                            log.warn("generate date/timestamp error! e="+e+" name="+name+" value="+value+" dataType="+MetadataDataType.findByValue(dataType));
                            throw e;   
                        }
                    }                     
                    break;
            }
        }
        
        if ( found == 0 )
            throw new Exception("all metadata value are empty!");
    }
    
    protected List<Map<String, Object>> generateContentData(Dataobject obj, Map<String,Object> contentInfo, List<FrontendMetadata> metadataList)
    {
        List<Map<String, Object>> newJsonMapList = new ArrayList<>();
        List<String> contentIds = (List<String>)contentInfo.get("contentIds");
        List<String> contentNames = (List<String>)contentInfo.get("contentNames");
        List<ByteBuffer> contentBinaries = (List<ByteBuffer>)contentInfo.get("contentBinaries");
        List<Boolean> isContentInSystems = (List<Boolean>)contentInfo.get("isContentInSystems");
        String contentText;
        String encoding;
        long contentSize;
        String contentLocation;
        
        for(int i=0;i<contentIds.size();i++)
        {            
            Map<String,Object> newJsonMap = new HashMap<>();
            
            newJsonMap.put("content_id", contentIds.get(i));
            newJsonMap.put("content_name", contentNames.get(i));
            
            contentLocation = Util.findSingleValueContentMetadataValue(contentIds.get(i),"content_location",metadataList);
            newJsonMap.put("content_location", contentLocation);
            
            contentSize = Long.parseLong(Util.findSingleValueContentMetadataValue(contentIds.get(i),"content_size",metadataList));
            newJsonMap.put("content_size", contentSize);
                        
            encoding = Util.findSingleValueContentMetadataValue(contentIds.get(i),"encoding",metadataList);
            newJsonMap.put("encoding",encoding);
            
            ContentType contentType = ContentType.findByValue(Integer.parseInt(Util.findSingleValueContentMetadataValue(contentIds.get(i),"content_type",metadataList)));
            
            if ( contentBinaries.get(i) != null )
                contentText = Util.getContentTextFromBinary(contentType, contentBinaries.get(i),encoding);
            else
                contentText = Util.findSingleValueContentMetadataValue(contentIds.get(i),"content_text",metadataList);
            
            newJsonMap.put("content_text",contentText);
            newJsonMap.put("mime_type",Util.findSingleValueContentMetadataValue(contentIds.get(i),"mime_type",metadataList));
            newJsonMap.put("content_type",contentType.getValue());
            newJsonMap.put("encoding",Util.findSingleValueContentMetadataValue(contentIds.get(i),"encoding",metadataList));
            newJsonMap.put("is_content_in_system",isContentInSystems.get(i));
            
            newJsonMapList.add(newJsonMap);
        }
        
        return newJsonMapList;
    }
}
