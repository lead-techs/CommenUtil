
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
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.enumeration.VideoExtractionStatusType;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
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
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.util.DataServiceUtil;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;

public class VideoFileProcessor extends Processor
{
    static final Logger log = Logger.getLogger("FileProcessor");
    private String objectStoreServiceProvider;
    private Map<String,String> objectStoreServiceProviderProperties;
    private Map<String,Object> taskParametersMap;
    
    private boolean processSubFolder = true;
    private boolean useInclusiveDocumentDiscoveryRule = true;
    private String documentIncludingRegularExpression = "";
    private String documentExcludingRegularExpression = "";
    private long maxFileSizeForCopyContentToSystem = 1200;
    private boolean copyContentToSystem = true;
    
    public VideoFileProcessor(CommonServiceConnector csConn,CommonService.Client csClient,DataServiceConnector dsConn,DataService.Client dsClient,Job newJob,DatasourceType datasourceType,ComputingNode computingNode,Counter serviceCounter)
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
     
    public VideoFileProcessor(DatasourceType datasourceType)
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
            int datasourceId = job.getDatasourceId();

            datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),datasourceId).array());
            datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());

            getConfig(dataService,datasource,job);             

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
        int sourceApplicationId = 0;
        int datasourceType = CommonKeys.DATASOURCE_TYPE_VIDEO_FILE_SERVER; // video file server
        int datasourceId = 0;

        int caseId = 0;
        int userId = 0;
        String cameraCode = "";
        int cameraId;
        boolean createIfNotExists = true;
        String cameraRegularExpression;
        boolean hasVideoCapturedTime;
        String videoCapturedTimeRegularExpression;
        String videoCapturedTimeFormat;
       
        objectStorageServiceInstance = (ServiceInstance)Tool.deserializeObject(commonService.getObjStorageSI(job.getOrganizationId(),job.getTargetRepositoryId()).array());
        objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
        objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
        
        datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),job.getDatasourceId()).array());
        datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());

        if ( computingNode.getNodeOsType() != ComputingNodeOSType.Windows.getValue() )
            Util.mountFileServerToLocalFolder(datasourceConnection,datasource,null);
              
        JSONObject configJson = new JSONObject(job.getTaskConfig());
        
        caseId = configJson.optInt("caseId");
        userId = configJson.optInt("userId");
        datasourceId = job.getDatasourceId();
        
        cameraRegularExpression = configJson.optString("cameraRegularExpression");
        videoCapturedTimeRegularExpression = configJson.optString("videoCapturedTimeRegularExpression");
        videoCapturedTimeFormat = configJson.optString("videoCapturedTimeFormat");

        hasVideoCapturedTime = (int)configJson.optInt("hasVideoCapturedTime")==1;
        log.info("2222 hasVideoCapturedTime =" + hasVideoCapturedTime);
 
        int k = 0;
        
        List<String> archiveFileList = getArchiveFileList(job.getTaskConfig());
        
        Map<String, String> metadata = new HashMap<>();

        metadata.put("VIDEO_CASE_ID", String.valueOf(caseId));
        metadata.put("VIDEO_IS_EXTRACTED_OBJECT_INFO", "false");

        metadata.put("VIDEO_UPLOAD_USER_ID", String.valueOf(userId));
        metadata.put("VIDEO_UPLOAD_TIME", String.valueOf(new Date().getTime()));
        metadata.put("VIDEO_EXTRACTION_STATUS", String.valueOf(VideoExtractionStatusType.NOT_EXTRACTED.getValue()));
         
        int videoToImageFrequencyType = configJson.optInt("videoToImageFrequencyType");

        Boolean extractVideoObjectNow = configJson.optInt("extractVideoObjectNow")==1;
        int processTypeId = CommonKeys.PROCESS_TYPE_FOR_EXTRACT_VIDEO_OBJECT;
        
        int processedItem = 0;
        List<String> dataobjectIds = new ArrayList<>();
        List<String> videoFilenames = new ArrayList<>();        
        
        for (String archiveFile : archiveFileList) 
        {
            processedItem++;
            
            log.info(" processing archivefile ="+archiveFile+", i="+processedItem+" total="+archiveFileList.size());
            
            String videoId = String.format("%d-%s", caseId, archiveFile);
            
            log.info(" 3 videoId=" + videoId);
            metadata.put("VIDEO_ID", videoId);

            metadata.put("VIDEO_FILE_FORMAT", "");
            metadata.put("VIDEO_FILE_NAME", String.valueOf(archiveFile));
            metadata.put("VIDEO_FILE_URL", archiveFile);
 
            cameraCode = Tool.extractValueByRE(archiveFile, cameraRegularExpression, 1);
            cameraCode = Tool.onlyKeepLetterOrNumber(cameraCode);
            
            Map<String, String> parameters = new HashMap<>();
            parameters.put("caseId",String.valueOf(caseId));
            parameters.put("description",cameraCode);
            
            cameraId =  dataService.getCameraIdByName(job.getOrganizationId(), cameraCode, parameters,createIfNotExists); 
    
            metadata.put("VIDEO_CAMERA_ID", String.valueOf(cameraId));
            metadata.put("VIDEO_CAMERA_INFO", "");

            if (hasVideoCapturedTime) 
            {
                String videoCapturedTimeStr = Tool.extractValueByRE(archiveFile, videoCapturedTimeRegularExpression, 1);
                                
                log.info("2222 videoStartTime=" + videoCapturedTimeStr);
                
                Date videoCapturedTime = Tool.convertStringToDate(videoCapturedTimeStr, videoCapturedTimeFormat);
                 
                metadata.put("VIDEO_START_TIME", String.valueOf(videoCapturedTime.getTime()));
            }
            else 
                metadata.put("VIDEO_START_TIME", String.valueOf(-8 * 3600 * 1000)); // utc time set to 0

            metadata.put("VIDEO_END_TIME", String.valueOf(-8 * 3600 * 1000));

            byte[] contentStream = FileUtil.readFileToByteBuffer(new File(archiveFile)).array();

            String dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(), sourceApplicationId, datasourceType, datasourceId, videoId, job.targetRepositoryId);

            dataobjectIds.add(dataobjectId);
            videoFilenames.add(archiveFile);
            
            DataServiceUtil.storeVideoDataobject(CommonKeys.DATAOBJECT_TYPE_FOR_VIDEO_FILE, dataobjectId, computingNode.getNodeOsType(), ByteBuffer.wrap(contentStream), dataService, commonService, job.getOrganizationId(), sourceApplicationId, datasourceType, datasourceId, archiveFile, archiveFile, job.getTargetRepositoryId(), false, metadata);
        }
      
        if ( extractVideoObjectNow )
            dataService.createExtractVideoObjectJob(job.getOrganizationId(), job.getTargetRepositoryId(), processTypeId, dataobjectIds, videoFilenames, String.valueOf(caseId), videoToImageFrequencyType);   
        
        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(),job.getId(),TaskJobStatus.COMPLETE.getValue(),"");
    }   
       
    private List<String> getArchiveFileList(String configStr)
    {
        List<String> list = new ArrayList<>();
         
        JSONObject configJson = new JSONObject(configStr);

        JSONArray array = configJson.optJSONArray("archiveFiles");

        Iterator<Object> it = array.iterator();

        while (it.hasNext())
        {
            JSONObject obj = (JSONObject)it.next();
            list.add(obj.optString("archiveFile"));
        }
        
        return list;
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
        return CommonKeys.DATAOBJECT_TYPE_FOR_VIDEO_FILE; // dataobject id for file in dataobject_type table
    }
    
    @Override
    protected void getConfig(DataService.Client dataService,Datasource datasource,Job job) throws Exception
    {
        JSONObject configJson = new JSONObject(job.getTaskConfig());
 
        useInclusiveDocumentDiscoveryRule = true;
        documentIncludingRegularExpression = configJson.optString("fileTypeRegularExpression");
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