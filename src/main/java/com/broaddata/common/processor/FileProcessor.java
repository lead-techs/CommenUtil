
/*
 * FileProcessor.java
 *
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.enumeration.JobItemProcessingStatisticType;
import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.DatasourceEndTask;
import com.broaddata.common.model.organization.Metadata;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.util.CAUtil;
import com.broaddata.common.util.CommonKeys;
import com.broaddata.common.util.CommonServiceConnector;
import com.broaddata.common.util.ContentStoreUtil;
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;

public class FileProcessor extends Processor
{
    static final Logger log = Logger.getLogger("FileProcessor");
    private String objectStoreServiceProvider;
    private Map<String,String> objectStoreServiceProviderProperties;
    private Map<String,Object> taskParametersMap;
    
    private boolean processSubFolder;
    private boolean useInclusiveDocumentDiscoveryRule;
    private String documentIncludingRegularExpression;
    private String documentExcludingRegularExpression;
    private long maxFileSizeForCopyContentToSystem;
    private boolean copyContentToSystem;
    
    public FileProcessor(CommonServiceConnector csConn,CommonService.Client csClient,DataServiceConnector dsConn,DataService.Client dsClient,Job newJob,DatasourceType datasourceType,ComputingNode computingNode,Counter serviceCounter)
    {
        this.job = newJob;
        this.commonService = csClient;
        this.dataService = dsClient;
        this.datasourceType = datasourceType; 
        this.computingNode = computingNode;
        this.csConn = csConn;
        this.dsConn = dsConn;
        this.serviceCounter = serviceCounter;
    }
     
    public FileProcessor(DatasourceType datasourceType)
    {
        this.datasourceType = datasourceType;
    }
     
    @Override
    protected void beforeStoringToServer(DataobjectInfo dataobject) throws Exception 
    {
        return; // for sub-class to override
    }
            
    @Override
    protected void discoverDataobject() throws Exception
    {
        List<DiscoveredDataobjectInfo> dataInfoList;    
        List<String> datasourceIds;
        String UNCPath;
        String localFolder;
               
        try
        {
            datasourceIds = Util.getConfigData(job.getTaskConfig(),CommonKeys.DATASOURCE_NODE);
            
            for( String datasourceIdStr:datasourceIds )
            {
                int datasourceId = Integer.valueOf(datasourceIdStr);

                datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),datasourceId).array());
                datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());
                                
                getTaskParameters(job.getTaskConfig());            
                
                Date businessDate = Tool.convertStringToDate(job.getBusinessDate(), "yyyy-MM-dd");
                
                if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                    localFolder = Tool.convertToWindowsPathFormat(Util.getSubFolder(datasourceConnection,datasource,businessDate));
                else
                    localFolder = Tool.convertToNonWindowsPathFormat(Util.mountFileServerToLocalFolder(datasourceConnection,datasource,businessDate));

                log.info( " local folder ="+localFolder);
                
                dataInfoList = new ArrayList<>();
                FileUtil.getFolderAllFiles(localFolder,dataInfoList,this,processSubFolder,false,job.getOrganizationId(),job.getId(),dataService,datasourceId, job, null); 
          
                log.info("files="+dataInfoList.size());
                
                if ( dataInfoList.size() > 0 )
                {
                    //generate documentId
                    Util.generateDocumentIdAndDatasource(dataInfoList,job,datasourceId);
                    
                    int retry = 0;
                    
                    while(true)
                    {
                        retry ++;
                        
                        try
                        {
                            log.info(" invoke dataservice.sendbackDiscoveryObj() ");
                            dataService.sendbackDiscoveredObj(job.getOrganizationId(),job.getId(),job.getTaskId(),datasourceId,dataInfoList, false);
                            break;
                        }
                        catch(Exception e)
                        {
                            log.error(" sendbackDiscoveryObj failed! e="+e);
                            Tool.SleepAWhileInMS(3*1000);
                            
                            if ( retry > 100 )
                                throw e;
                        }
                    }
                }
                else
                    dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.COMPLETE.getValue(), "");
            }
        }
        catch (Exception e) 
        {
            String errorInfo = String.format(" discoveData() failed! datasourceId=%d, e=%s, statcktrace=%s",datasource.getId(),e,ExceptionUtils.getStackTrace(e)); 
            dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);
            throw e;
        }
    }
    
    @Override
    protected void archiveDataobjects() throws Exception
    {   
        String originalSourcePath;
        boolean contentCopied;
        String  errorInfo;
        List<String> archiveFileList = Util.getConfigData(job.getTaskConfig(),CommonKeys.ARCHIVE_FILE_NODE);
        
        task = (DatasourceEndTask)Tool.deserializeObject(dataService.getDatasourceEndTask(job.getOrganizationId(),job.getTaskId()).array());
         
        objectStorageServiceInstance = (ServiceInstance)Tool.deserializeObject(commonService.getObjStorageSI(job.getOrganizationId(),job.getTargetRepositoryId()).array());
        objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
        objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
        
        datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),job.getDatasourceId()).array());
        datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());
        
        getConfig(dataService,datasource,job);       
        
        getTaskParameters(job.getTaskConfig());
        
        if ( computingNode.getNodeOsType() != ComputingNodeOSType.Windows.getValue() )
            Util.mountFileServerToLocalFolder(datasourceConnection,datasource,null);
              
        int k = 0;
        
        for( String archiveFile:archiveFileList )
        {
            k++;

            if ( k <= job.getAlreadyProcessedLine() )
            {
                log.info("already processed. skip! k="+k+" alreadyProcessed="+job.getAlreadyProcessedLine());
                continue;
            }
                
            log.info("archive file="+archiveFile);
            
            if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
            {
                archiveFile = Tool.convertToWindowsPathFormat(archiveFile);
                originalSourcePath = archiveFile;
            }
            else
            {
                archiveFile = Tool.convertToNonWindowsPathFormat(archiveFile);
                originalSourcePath = Tool.convertToNonWindowsPathFormat(Util.getOriginalSourcePath(datasourceConnection,datasource,archiveFile));
            }
            
            log.info("originalSourcePath="+originalSourcePath);
    
            try
            {
                contentCopied = false;
                DataobjectInfo obj = new DataobjectInfo();
                
                String dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(),job.getSourceApplicationId(),job.getDatasourceType(),job.getDatasourceId(),originalSourcePath,job.getTargetRepositoryId());
                
                obj.setDataobjectId(dataobjectId);
                obj.setSourceApplicationId(job.getSourceApplicationId());
                obj.setDatasourceType(job.getDatasourceType());
                obj.setDatasourceId(job.getDatasourceId());
                //obj.setSourcePath(archiveFile);
                obj.setSourcePath(originalSourcePath);  // set original file path
                obj.setName(FileUtil.getFileNameWithoutPath(archiveFile));
              
                // read file /*
                ByteBuffer contentStream = FileUtil.readFileToByteBuffer(new File(archiveFile));
                
                List<String> contentIds = new ArrayList<>();
                contentIds.add(DataIdentifier.generateContentId(contentStream));
                
                List<String> contentNames = new ArrayList<>();
                contentNames.add("main_content");
                
                List<ByteBuffer> contentBinaries = new ArrayList<>();
                contentBinaries.add(contentStream);
                
                List<Boolean> needToExtractContentTexts = new ArrayList<>();
                List<Boolean> needToSaveContentInSystems = new ArrayList<>();
                
                obj.setContentIds(contentIds);
                obj.setContentNames(contentNames);
                obj.setSize(contentStream.capacity());
                obj.setMetadataList(new ArrayList<FrontendMetadata>());
                obj.setIsPartialUpdate(false);
                obj.setNeedToSaveImmediately(false);
     
                List<String> contentLocations = new ArrayList<>();
                                             
                if ( copyContentToSystem )
                    contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_BLOB_STORE,contentIds.get(0)));
                else
                    contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_FILE_SERVER,archiveFile));
  
                // check if content already exists
                boolean isContentExisting = dataService.isContentExisting(job.getOrganizationId(),obj.getContentIds().get(0));

                log.info(" isContentExisting="+isContentExisting+" copyContentToSystem="+copyContentToSystem);
                
                if ( !isContentExisting && copyContentToSystem )
                {
                    if ( contentStream.capacity() <= maxFileSizeForCopyContentToSystem*1000000 ) // > xxx MB
                    {
                         ContentStoreUtil.storeContent(job.getOrganizationId(),dataService,computingNode.getNodeOsType(),contentIds.get(0),contentStream,objectStorageServiceInstance,objectStoreServiceProviderProperties);
                         contentCopied = true;
                    }
                    else
                        log.debug(" content is greater than maxfilesize contentId="+obj.getContentIds().get(0));
                }
                else
                {
                    if ( isContentExisting )
                    {
                        contentCopied = true;
                        log.debug(" content already exists in system ! contentId="+obj.getContentIds().get(0));
                    }
                    else
                        log.debug(" task no need to copy content to system! contentId="+obj.getContentIds().get(0));
                }                  
                                
                if ( !isContentExisting && !copyContentToSystem )
                    needToExtractContentTexts.add(true);
                else
                    needToExtractContentTexts.add(false);
                
                //get object metadata
                collectDataobjectMetadata(obj,archiveFile);
                                
                //get content metadata
                collectDataobjectContentMetadata(dataService,datasource,job,obj,contentIds,contentBinaries,contentLocations,needToExtractContentTexts,archiveFile);
                        
                // set dataobject type
                obj.setDataobjectType(getDataobjectTypeId(obj));
                needToSaveContentInSystems.add(contentCopied);
                obj.setSaveContentInSystems(needToSaveContentInSystems);
                                
                // for sub-class to override
                beforeStoringToServer(obj);
                
                List<DataobjectInfo> list = new ArrayList<>();
                list.add(obj);
        
                dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),list,job.getId(),false);
                
                dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 1);
            
                errorInfo = "";
            }
            catch (Exception e)
            {
                errorInfo = "archiveData() failed! archiveFile="+archiveFile+"　e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
                log.error(errorInfo);
                       
                dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(),job.getId(),TaskJobStatus.FAILED.getValue(),errorInfo);

                throw e;
            }
        }
        
        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(),job.getId(),TaskJobStatus.COMPLETE.getValue(),"");
    }   
       
    @Override
    protected void collectDataobjectMetadata(DataobjectInfo dataobject,String archiveFile) throws Exception
    {
        FrontendMetadata metadata;
        List<FrontendMetadata> metadataList = dataobject.getMetadataList();  
             
        metadata = new FrontendMetadata("stored_yearmonth",true);
        metadata.setSingleValue(Tool.getYearMonthStr(new Date()));
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("source_application_id",true);
        metadata.setSingleValue( String.valueOf(job.getSourceApplicationId()) );
        metadataList.add(metadata);        
        
        metadata = new FrontendMetadata("file_name",true);
        metadata.setSingleValue(dataobject.getName());
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("file_folder_name",true); 
        metadata.setSingleValue(FileUtil.getFolderName(dataobject.getSourcePath()));
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("file_fullpath",true); 
        metadata.setSingleValue(dataobject.getSourcePath());
        metadataList.add(metadata);       
        
        metadata = new FrontendMetadata("file_size",true); 
        metadata.setSingleValue(String.valueOf(dataobject.getSize()));
        metadataList.add(metadata);           
        
        metadata = new FrontendMetadata("file_ext",true); 
        metadata.setSingleValue( FileUtil.getFileExt(dataobject.getName()));
        metadataList.add(metadata);
        
        BasicFileAttributes fa = FileUtil.getFileBasicAttributes(archiveFile);
        
        if ( fa != null )
        {
            metadata = new FrontendMetadata("created_time",true); 
            metadata.setSingleValue(String.valueOf(fa.creationTime().toMillis()));
            metadataList.add(metadata);

            metadata = new FrontendMetadata("last_modified_time",true); 
            metadata.setSingleValue(String.valueOf(fa.lastModifiedTime().toMillis()));
            metadataList.add(metadata);           

            metadata = new FrontendMetadata("last_accessed_time",true); 
            metadata.setSingleValue(String.valueOf(fa.lastAccessTime().toMillis()));
            metadataList.add(metadata);
        }

        metadata = new FrontendMetadata("owner",true);
        metadata.setSingleValue(FileUtil.getFileOwner(archiveFile));
        metadataList.add(metadata);
        
        //metadata = new FrontendMetadata("owner",false);
        //List<String> list = new ArrayList();
        //list.add(FileUtil.getFileOwner(dataobject.getSourcePath()));
        //list.add(FileUtil.getFileOwner(dataobject.getSourcePath())+"copy");
        //metadata.setMultiValues(list);
        //metadataList.add(metadata);   
    }
    
    @Override
    protected void collectDataobjectContentMetadata(DataService.Client dataService,Datasource datasource,Job job, DataobjectInfo dataobject, List<String> contentIds, List<ByteBuffer> contentBinaries, List<String> contentLocations, List<Boolean> needToExtractContentTexts,String archiveFile) throws Exception
    {       
        FrontendMetadata metadata;
        List<FrontendMetadata> metadataList = dataobject.getMetadataList();
             
        String contentId = contentIds.get(0);  // file only has one content
        ByteBuffer contentBinary = contentBinaries.get(0);
        Boolean needToExtractContentText = needToExtractContentTexts.get(0);
        
        metadata = new FrontendMetadata("content_location",true);
        metadata.setSingleValue(contentLocations.get(0));
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("content_size",true); 
        metadata.setSingleValue( String.valueOf(contentBinary.capacity()));
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("encoding",true); 
        String encoding = CAUtil.detectEncoding(archiveFile);
        metadata.setSingleValue(encoding);
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("mime_type",true); 
        String mime_type = CAUtil.detectMimeType(archiveFile);
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
        
    @Override
    public void generateIndexMetadata(List<Map<String,Object>> metadataDefinitions,Map<String, Object> jsonMap, List<FrontendMetadata> metadataList, Dataobject obj, Map<String,Object> contentInfo,boolean needEncryption) throws Exception 
    {
        generateIndexMetadataDefault(metadataDefinitions,jsonMap,metadataList,obj,contentInfo,needEncryption);
        
        jsonMap.put("file_name",obj.getName());
        jsonMap.put("file_folder_name",Util.findSingleValueMetadataValue("file_folder_name",metadataList));
        jsonMap.put("file_fullpath",Util.findSingleValueMetadataValue("file_fullpath",metadataList));
        jsonMap.put("file_ext",Util.findSingleValueMetadataValue("file_ext",metadataList));
   
        String time = Util.findSingleValueMetadataValue("created_time",metadataList);
        if (time.length()>0)
            jsonMap.put("created_time",new Date(Long.parseLong(time)));

        time = Util.findSingleValueMetadataValue("last_modified_time",metadataList);
        if (time.length()>0)
            jsonMap.put("last_modified_time",new Date(Long.parseLong(time)));

        time = Util.findSingleValueMetadataValue("last_accessed_time",metadataList);
        if (time.length()>0)
            jsonMap.put("last_accessed_time",new Date(Long.parseLong(time)));

        jsonMap.put("owner",Util.findMutiValueMetadataValue("owner",metadataList));     
        
        if ( contentInfo.get("contentBinaries") != null )
            jsonMap.put("contents", generateContentData(obj,contentInfo,metadataList));
    }

    @Override
    protected int getDataobjectTypeId(DataobjectInfo obj) 
    {
        return CommonKeys.DATAOBJECT_TYPE_FOR_FILE; // dataobject id for file in dataobject_type table
    }
    
    @Override
    protected void getConfig(DataService.Client dataService,Datasource datasource,Job job) throws Exception
    {
       // for child class to over write
    }
       
    @Override
    public boolean validateDataobject(DiscoveredDataobjectInfo obj) 
    {
        Pattern pattern;
        boolean matched;      
        
        String filename = obj.getPath()+"/"+obj.getName();
                    
        try 
        {
            if ( useInclusiveDocumentDiscoveryRule )
            {
                if ( documentIncludingRegularExpression.length()>0)
                {
                    pattern  = Pattern.compile(documentIncludingRegularExpression);
                    matched = pattern.matcher(filename).matches();

                    return matched;                
                }
                else
                    return true;

            }
            else //exclude
            {
                if ( documentExcludingRegularExpression.length()>0 )
                {
                    pattern = Pattern.compile(documentExcludingRegularExpression);
                    matched = pattern.matcher(filename).matches();

                    return !matched;                
                }
                else
                    return true;
            }            
        }
        catch(Exception e)
        {
            log.error("validateDataobject() failed! DiscoveredDataobjectInfo obj="+obj.getDataobjectId());
            return false;
        }
    }
    
    private void getTaskParameters(String taskConfig) 
    {    
        Map<String,Object> map = new HashMap<>();
        
        try
        {
            Document metadataXmlDoc = Tool.getXmlDocument(taskConfig);

            String nodeName = String.format("//taskConfig/parameters/parameter");
            List<Element> nodes = metadataXmlDoc.selectNodes(nodeName);

            for( Element node: nodes)
            {
                String name = node.element("name").getTextTrim();
                String value = node.element("value").getTextTrim();

                switch(name)
                {
                    case "processSubFolder":
                        processSubFolder = value.equals("1");
                        break;
                    case "useInclusiveDocumentDiscoveryRule":
                        useInclusiveDocumentDiscoveryRule = value.equals("1");
                        break;
                    case "documentIncludingRegularExpression":
                        documentIncludingRegularExpression = value;
                        break;
                    case "documentExcludingRegularExpression":
                        documentExcludingRegularExpression = value;
                        break;        
                    case "copyContentToSystem":
                        copyContentToSystem = value.equals("1");
                        break;
                    case "maxFileSizeForCopyContentToSystem":
                        maxFileSizeForCopyContentToSystem = Long.parseLong(value);
                        break;
                }
            }
        }
        catch (Exception e) {
             throw e;
        }
    }
}