/*
 * PstFileUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import com.pff.PSTAttachment;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer; 
import java.util.Date;
import java.util.List;

import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.enumeration.EmailFieldNames;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import org.apache.commons.lang.exception.ExceptionUtils;
import com.broaddata.common.model.enumeration.ContentType;
import java.util.Vector;

 
public class PstFileUtil 
{
    static final Logger log = Logger.getLogger("StructuredDataFileUtil");
     // public static Map<String,Object> importPstFileToSystem(Map<String,Object> parameters,int organizationId,long jobId,String filename,DataService.Client dataService,CommonService.Client commonService,int datasourceType, int datasourceId, int sourceApplicationId,int targetRepositoryId,ClassWithCallBack callback,int alreadyProcessedLine)
   
    public static Map<String,Object> importPstFileToSystem(String filename,Map<String,Object> parameters)
    {
        int status;
        StringBuilder errorInfo = new StringBuilder();
        int processedLine = 0;

        Map<String,Object> importResults = new HashMap<>();
        List<Map<String,String>> emailList = new ArrayList<>();
        List<Map<String,ByteBuffer>> emailAttachmentList = new ArrayList<>();
        
        try
        {
            processPstFolder(filename,new PSTFile(filename).getRootFolder(),emailList,emailAttachmentList,parameters);
            
            if ( emailList.size() > 0 )
            {
                saveDataToEdf(filename,emailList,emailAttachmentList,parameters);
            }
        }
        catch(Throwable e)
        {
            log.error("importExcelFileContentToSystem() failed! e="+e);
            errorInfo.append(String.format("importExcelFileToSystem() failed! e=%s",ExceptionUtils.getStackTrace(e)));
        }
        
        if ( errorInfo.toString().isEmpty() )
            status = TaskJobStatus.COMPLETE.getValue();
        else
            status = TaskJobStatus.FAILED.getValue();
                
        importResults.put("status", status);
        importResults.put("totalProcessedLine", processedLine); 
        importResults.put("errorInfo", errorInfo.toString());
                
        return importResults;
    }
  
    private static void processPstFolder(String filename,PSTFolder folder,List<Map<String,String>> emailList,List<Map<String,ByteBuffer>> emailAttachmentList,Map<String,Object> parameters) throws Exception
    {
        // go through the folders...
        if (folder.hasSubfolders()) 
        {
            Vector<PSTFolder> childFolders = folder.getSubFolders();

            for (PSTFolder childFolder : childFolders)
                processPstFolder(filename,childFolder,emailList,emailAttachmentList,parameters);
        }
    
        log.info(" process folder="+folder.getDisplayName()); 
        
        // process emails for this folder
        if (folder.getContentCount() > 0)
        {
            PSTMessage email = (PSTMessage) folder.getNextChild();

            while (email != null) 
            {
                HashMap<String,String> msgData = new HashMap<>();
                HashMap<String,ByteBuffer> msgAttachmentData = new HashMap<>();
          
                msgData.put(EmailFieldNames.MESSAGE_ID.name(),String.valueOf(email.getDescriptorNodeId()));
                msgData.put(EmailFieldNames.SUBJECT.name(),email.getSubject());
                Date date = email.getMessageDeliveryTime();
                msgData.put(EmailFieldNames.SEND_TIME.name(),String.valueOf(date.getTime()));
                msgData.put(EmailFieldNames.FOLDER.name(),folder.getDisplayName());
                msgData.put(EmailFieldNames.SIZE.name(),String.valueOf(email.getMessageSize()));

                // Email Address
                msgData.put(EmailFieldNames.FROM.name(), email.getSenderEmailAddress());
                msgData.put(EmailFieldNames.TO.name(), email.getDisplayTo()); 
                msgData.put(EmailFieldNames.CC.name(), email.getDisplayCC());
                msgData.put(EmailFieldNames.BCC.name(), email.getDisplayBCC());

                // Body
                msgData.put(EmailFieldNames.BODY.name(), email.getBody());

                // Attachments
                processAttachments(email,msgData,msgAttachmentData);

                emailList.add(msgData);
                emailAttachmentList.add(msgAttachmentData);
                
                if ( emailList.size() > 600 )
                {
                    saveDataToEdf(filename,emailList,emailAttachmentList,parameters);
                    emailList.clear();
                    emailAttachmentList.clear();
                }
                
                email = (PSTMessage) folder.getNextChild();
            }
        }
    }

    private static void saveDataToEdf(String filename,List<Map<String,String>> emailList,List<Map<String,ByteBuffer>> emailAttachmentList,Map<String,Object> parameters) throws Exception
    {
        List<DataobjectInfo> dataobjectInfoList = new ArrayList<>();
        String dataobjectId;
        DataobjectInfo dataobject;
        List<FrontendMetadata> metadataList;
        FrontendMetadata metadata;
        List<String> contentIds;
        List<String> contentNames;
        List<Boolean> saveContentInSystemList;
   
        int organizationId = (int)parameters.get("organizationId");
        long jobId = (long)parameters.get("jobId");
        DataService.Client dataService = (DataService.Client)parameters.get("dataService");
        CommonService.Client commonService = (CommonService.Client)parameters.get("commonService");
        int datasourceType = (int)parameters.get("datasourceType");
        int datasourceId = (int)parameters.get("datasourceId");
        int sourceApplicationId = (int)parameters.get("sourceApplicationId");
        int targetRepositoryId = (int)parameters.get("targetRepositoryId");
        ClassWithCallBack callback = (ClassWithCallBack)parameters.get("callback");
        int alreadyProcessedLine = (int)parameters.get("alreadyProcessedLine");
        int targetRepositoryType = (int)parameters.get("targetRepositoryType");
        int targetDataobjectType = (int)parameters.get("targetDataobjectType");
        ComputingNode computingNode = (ComputingNode)parameters.get("computingNode");
        ServiceInstance objectStorageServiceInstance = (ServiceInstance)parameters.get("objectStorageServiceInstance");
        Map<String,String> objectStoreServiceProviderProperties = (Map<String,String>)parameters.get("objectStoreServiceProviderProperties");
       
        try
        {
            for(int i=0;i<emailList.size();i++)
            {
                Map<String,String> emailData = emailList.get(i);
                
                String primaryKeyValue = String.format("%s-%s",filename,(String)emailData.get(EmailFieldNames.MESSAGE_ID.name()));
                
                dataobjectId = DataIdentifier.generateDataobjectId(organizationId,sourceApplicationId,datasourceType,0,primaryKeyValue,targetRepositoryId);
                dataobject = new DataobjectInfo();
                dataobject.setDataobjectId(dataobjectId);
                dataobject.setSourceApplicationId(sourceApplicationId);
                dataobject.setDatasourceType(datasourceType);
                dataobject.setDatasourceId(datasourceId);
                dataobject.setDataobjectType(targetDataobjectType);
                dataobject.setSize(0);       
                dataobject.setName(String.format("%s-%s-%s",filename,emailData.get(EmailFieldNames.FOLDER.name()),emailData.get(EmailFieldNames.SUBJECT.name())));
                dataobject.setSourcePath(String.format("%s-%s-%s",filename,emailData.get(EmailFieldNames.FOLDER.name()),emailData.get(EmailFieldNames.SUBJECT.name())));
                dataobject.setIsPartialUpdate(false);
                dataobject.setNeedToSaveImmediately(false);

                metadataList = new ArrayList<>();
                
                for(Map.Entry<String,String> entry : emailData.entrySet())
                {
                    String fieldName = entry.getKey().toLowerCase(); 
                    String value = entry.getValue();
  
                    metadata = new FrontendMetadata(fieldName,true);
                    metadata.setSingleValue(value);
                    metadataList.add(metadata);
                }
                
                contentIds = new ArrayList<>();
                contentNames = new ArrayList<>();
                saveContentInSystemList = new ArrayList<>();
                
                ByteBuffer contentBinary = MailUtil.saveToEmlFile(emailData,emailAttachmentList.get(i));
                
                String contentId = DataIdentifier.generateContentId(contentBinary);
                String contentName = String.format("%s.eml",emailData.get(EmailFieldNames.SUBJECT.name()));

                contentNames.add(contentName);
                contentIds.add(contentId);
                saveContentInSystemList.add(Boolean.TRUE);

                boolean isContentExisting = dataService.isContentExisting(organizationId,contentId);                    
                String contentLocation = String.format("%s:%s",CommonKeys.CONTENT_LOCATION_BLOB_STORE,contentId);

                if ( !isContentExisting )
                    ContentStoreUtil.storeContent(organizationId,dataService,computingNode.getNodeOsType(),contentId,contentBinary,objectStorageServiceInstance,objectStoreServiceProviderProperties);

                metadata = new FrontendMetadata("content_location",true); 
                metadata.setSingleValue(contentLocation);
                metadata.setContentId(contentId);
                metadataList.add(metadata);

                metadata = new FrontendMetadata("content_size",true); 
                metadata.setSingleValue( String.valueOf(contentBinary.capacity()));
                metadata.setContentId(contentId);            
                metadataList.add(metadata);

                metadata = new FrontendMetadata("encoding",true); 
                String encoding = CAUtil.detectEncoding(contentBinary);
                metadata.setSingleValue(encoding);
                metadata.setContentId(contentId);            
                metadataList.add(metadata);

                metadata = new FrontendMetadata("mime_type",true); 
                String mime_type = CAUtil.detectMimeType(contentBinary);
                metadata.setSingleValue(mime_type);
                metadata.setContentId(contentId);            
                metadataList.add(metadata);

                metadata = new FrontendMetadata("content_type",true); 
                ContentType contentType = Tool.getContentType(mime_type);
                metadata.setSingleValue( String.valueOf(contentType.getValue()) );
                metadata.setContentId(contentId);            
                metadataList.add(metadata);    
                                    
                int j=0;
                for(Map.Entry<String,ByteBuffer> entry : emailAttachmentList.get(i).entrySet())
                {
                    j++;
                    String fileName = entry.getKey();
                    contentBinary = entry.getValue();
        
                    contentId = DataIdentifier.generateContentId(contentBinary);   
                    contentName = String.format("%s_%d_%s",Util.getBundleMessage("attachment"),j,fileName);
                    
                    contentNames.add(contentName);
                    contentIds.add(contentId);
                    saveContentInSystemList.add(Boolean.TRUE);
                    
                    isContentExisting = dataService.isContentExisting(organizationId,contentId);                    
                    contentLocation = String.format("%s:%s",CommonKeys.CONTENT_LOCATION_BLOB_STORE,contentId);
                    
                    if ( !isContentExisting )
                        ContentStoreUtil.storeContent(organizationId,dataService,computingNode.getNodeOsType(),contentId,contentBinary,objectStorageServiceInstance,objectStoreServiceProviderProperties);
       
                    metadata = new FrontendMetadata("content_location",true); 
                    metadata.setSingleValue(contentLocation);
                    metadata.setContentId(contentId);
                    metadataList.add(metadata);

                    metadata = new FrontendMetadata("content_size",true); 
                    metadata.setSingleValue( String.valueOf(contentBinary.capacity()));
                    metadata.setContentId(contentId);            
                    metadataList.add(metadata);

                    metadata = new FrontendMetadata("encoding",true); 
                    encoding = CAUtil.detectEncoding(contentBinary);
                    metadata.setSingleValue(encoding);
                    metadata.setContentId(contentId);            
                    metadataList.add(metadata);

                    metadata = new FrontendMetadata("mime_type",true); 
                    mime_type = CAUtil.detectMimeType(contentBinary);
                    metadata.setSingleValue(mime_type);
                    metadata.setContentId(contentId);            
                    metadataList.add(metadata);

                    metadata = new FrontendMetadata("content_type",true); 
                    contentType = Tool.getContentType(mime_type);
                    metadata.setSingleValue( String.valueOf(contentType.getValue()) );
                    metadata.setContentId(contentId);            
                    metadataList.add(metadata);          
                }           
                
                dataobject.setContentIds(contentIds);
                dataobject.setContentNames(contentNames);
                dataobject.setSaveContentInSystems(saveContentInSystemList);
     
                dataobject.setMetadataList(metadataList);  
                
                dataobjectInfoList.add(dataobject);
            }
            
            int retry = 0;

            while(true)
            {
                retry ++;

                try
                {
                    dataService.storeDataobjects(organizationId,targetRepositoryType,targetRepositoryId,dataobjectInfoList,jobId,false);
                    break;
                }
                catch(Exception e) 
                {   
                    log.error("dataService.storeDataobjects failed! e="+e);

                    Tool.SleepAWhile(1, 0);
                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                    {
                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                        throw e;
                    }
                }
            }
        }
        catch(Exception e)
        {
            log.error(" saveDataToEdf failed! e="+e);
            throw e;
        }
    }   
    
    private static void processAttachments(PSTMessage email, HashMap<String,String> msgData, HashMap<String,ByteBuffer> msgAttachmentData) throws Exception 
    {
        ArrayList<String> nameList = new ArrayList<>();
        ArrayList<String> contentList = new ArrayList<>();

        try 
        {
            int numberOfAttachments = email.getNumberOfAttachments();
            
            for (int i=0; i< numberOfAttachments; i++) 
            {
                PSTAttachment attach = email.getAttachment(i);
 
                String fileName = attach.getLongFilename();
                if (fileName.isEmpty())
                    fileName = attach.getFilename();
                 
                String fileContent = "";
               /* Tika tika = new Tika();
                try
                {
                    fileContent = tika.parseToString(attach.getFileInputStream());
                }
                catch(Throwable t )
                {
                    log.error(" tika parseToString failed! t="+t);
                }*/
                
                nameList.add(fileName);
                contentList.add(fileContent);
                
                ByteBuffer buf = FileUtil.readFileToByteBuffer(attach.getFileInputStream());
                msgAttachmentData.put(fileName, buf);
            }

            msgData.put(EmailFieldNames.ATTACHMENT_COUNT.name(), String.valueOf(nameList.size()));

            if (!nameList.isEmpty())
            {
                String nameStr = Tool.convertStringArrayToSingleString(nameList,",");
                msgData.put(EmailFieldNames.ATTACHMENT_NAMES.name(), nameStr);
            }

            if (!contentList.isEmpty()) 
            {
                String contentStr =  Tool.convertStringArrayToSingleString(contentList,",");
                msgData.put(EmailFieldNames.ATTACHMENT_CONTENT.name(), contentStr);
            }

        }
        catch (Exception e)
        {
            log.error(" processAttachments() failed! e="+e);
            throw e;
        }
    }
 
}
    
