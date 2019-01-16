/*
 * ImageServiceUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import com.broaddata.common.thrift.imageservice.ImageService;

public class imageServiceUtil 
{      
    static final Logger log=Logger.getLogger("CAUtil");      

    public static void storeVideoDataobject(int targetDataobjectType,String dataobjectId,int nodeOSType,ByteBuffer contentStream,ImageService.Client imageService,CommonService.Client commonService,int organizationId,int sourceApplicationId,int datasourceType,int datasourceId,String filename,String originalSourcePath,int targetRepositoryId,boolean needToExtractContentTexts,Map<String,String> metadataData) throws Exception
    {
        ServiceInstance objectStorageServiceInstance;
        String errorInfo = "";
        try
        {
            log.info("111111111111 store file="+filename);
            
            objectStorageServiceInstance = (ServiceInstance)Tool.deserializeObject(commonService.getObjStorageSI(organizationId,targetRepositoryId).array());
            Map<String,String> objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
        
            boolean contentCopied = false;
            DataobjectInfo obj = new DataobjectInfo();

            obj.setDataobjectId(dataobjectId);
            obj.setSourceApplicationId(sourceApplicationId);
            obj.setDatasourceType(datasourceType);
            obj.setDatasourceId(datasourceId);
            //obj.setSourcePath(archiveFile);
            obj.setSourcePath(originalSourcePath);  // set original file path
            obj.setName(FileUtil.getFileNameWithoutPath(filename));
            
            List<String> contentIds = new ArrayList<>();
            contentIds.add(DataIdentifier.generateContentId(contentStream));

            List<String> contentNames = new ArrayList<>();
            contentNames.add("main_content");

            List<ByteBuffer> contentBinaries = new ArrayList<>();
            contentBinaries.add(contentStream);
 
            List<Boolean> needToSaveContentInSystems = new ArrayList<>();

            obj.setContentIds(contentIds);
            obj.setContentNames(contentNames);
            obj.setSize(contentStream.capacity());
            obj.setMetadataList(new ArrayList<FrontendMetadata>());
            obj.setIsPartialUpdate(false);
            obj.setNeedToSaveImmediately(false);

            obj.setMetadataList(new ArrayList<FrontendMetadata>());
                               
            FrontendMetadata metadata;
            List<FrontendMetadata> metadataList = obj.getMetadataList();  
            
            for(Map.Entry<String,String> entry : metadataData.entrySet())
            {
                metadata = new FrontendMetadata(entry.getKey(),true);
                metadata.setSingleValue(entry.getValue());
                metadataList.add(metadata);
            }         
 
            List<String> contentLocations = new ArrayList<>();
 
            contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_BLOB_STORE,contentIds.get(0)));
          
            // check if content already exists
            //boolean isContentExisting = imageService.isContentExisting(organizationId,obj.getContentIds().get(0));

            boolean isContentExisting = false;
            
            log.info(" isContentExisting="+isContentExisting);

            if ( !isContentExisting )
            {
                // ContentStoreUtil.storeContent(organizationId,imageService,nodeOSType,contentIds.get(0),contentStream,objectStorageServiceInstance,objectStoreServiceProviderProperties);
                contentCopied = true;
            }
            else
            {
                if ( isContentExisting )
                {
                    contentCopied = true;
                    log.info(" content already exists in system ! contentId="+obj.getContentIds().get(0));
                }
                else
                    log.debug(" task no need to copy content to system! contentId="+obj.getContentIds().get(0));
            }                  
  
            //get object metadata
            collectDataobjectMetadata(obj,sourceApplicationId);

            //get content metadata
            collectDataobjectContentMetadata(obj,contentIds,contentBinaries,contentLocations,needToExtractContentTexts);

            // set dataobject type
            obj.setDataobjectType(targetDataobjectType);
            needToSaveContentInSystems.add(contentCopied);
            obj.setSaveContentInSystems(needToSaveContentInSystems);
 
            List<DataobjectInfo> list = new ArrayList<>();
            list.add(obj);

            // imageService.storeDataobjects(organizationId,TargetRepositoryType.DATA_REPOSITORY.getValue(),targetRepositoryId,list,0,true);
        }
        catch (Exception e)
        {
            errorInfo = "archiveData() failed! archiveFile="+filename+"　e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
 
            throw e;
        }
    }
  
    private static void collectDataobjectMetadata(DataobjectInfo dataobject,int sourceApplicationId) throws Exception
    {
        FrontendMetadata metadata;
        List<FrontendMetadata> metadataList = dataobject.getMetadataList();  
             
        metadata = new FrontendMetadata("stored_yearmonth",true);
        metadata.setSingleValue(Tool.getYearMonthStr(new Date()));
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("source_application_id",true);
        metadata.setSingleValue( String.valueOf(sourceApplicationId) );
        metadataList.add(metadata);       
    }
 
    private static void collectDataobjectContentMetadata(DataobjectInfo dataobject, List<String> contentIds, List<ByteBuffer> contentBinaries, List<String> contentLocations, boolean needToExtractContentText) throws Exception
    {       
        FrontendMetadata metadata;
        List<FrontendMetadata> metadataList = dataobject.getMetadataList();
             
        String contentId = contentIds.get(0);  // file only has one content
        ByteBuffer contentBinary = contentBinaries.get(0);
    
        metadata = new FrontendMetadata("content_location",true);
        metadata.setSingleValue(contentLocations.get(0));
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("content_size",true); 
        metadata.setSingleValue( String.valueOf(contentBinary.capacity()));
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("encoding",true); 
        String encoding = CAUtil.detectEncoding(contentBinary.array());
        metadata.setSingleValue(encoding);
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("mime_type",true); 
        String mime_type = CAUtil.detectMimeType(contentBinary.array());
        metadata.setSingleValue(mime_type);
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("content_type",true);
        ContentType contentType = Tool.getContentType(mime_type);
        metadata.setSingleValue( String.valueOf(contentType.getValue()) );
        metadata.setContentId(contentId);        
        metadataList.add(metadata);
         
        if ( needToExtractContentText )
        {
            metadata = new FrontendMetadata("content_text",true);
            String contentText = Util.getContentTextFromBinary(contentType, contentBinary,encoding);           
            metadata.setSingleValue(contentText);
            metadata.setContentId(contentId);            
            metadataList.add(metadata);                
        }           
    }
    
}
