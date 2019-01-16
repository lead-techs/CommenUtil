/*
 * SearchEngineProcessor.java
 *
 */

package com.broaddata.common.processor;

import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.enumeration.JobItemProcessingStatisticType;
import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
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
import com.broaddata.common.util.ContentStoreUtil;
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.SEUtil;
import static com.broaddata.common.util.SEUtil.getUrlCharSet;
import static com.broaddata.common.util.SEUtil.getUrlContentString;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.jsoup.Jsoup;

public class SearchEngineProcessor extends Processor
{
    static final Logger log = Logger.getLogger("FileProcessor");
 
    private String objectStoreServiceProvider;
    private Map<String,String> objectStoreServiceProviderProperties;   
    private long maxFileSizeForCopyContentToSystem;
    private boolean copyContentToSystem;
    
    public SearchEngineProcessor(CommonService.Client csClient,DataService.Client dsClient,Job newJob,DatasourceType datasourceType,ComputingNode computingNode,Counter serviceCounter)
    {
        this.job = newJob;
        this.commonService = csClient;
        this.dataService = dsClient;
        this.datasourceType = datasourceType; 
        this.computingNode = computingNode;
        this.serviceCounter = serviceCounter;
    }
     
    public SearchEngineProcessor(DatasourceType datasourceType)
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
                
                dataInfoList = new ArrayList<>();
                
                int searchEngineInstanceId = Integer.parseInt(Util.getDatasourceConnectionProperty(datasourceConnection, "searchEngine"));
                String keyword = Util.getDatasourceProperty(datasource, "searchKeyword");
                int maxExpectedNumberOfResults = Integer.parseInt(Util.getDatasourceProperty(datasource, "maxExpectedNumberOfResults"));
                
                SEUtil.getWebPagesFromSearchEngine(dataInfoList,searchEngineInstanceId,keyword,maxExpectedNumberOfResults);
                                
                log.info("found web page number ="+dataInfoList.size());
                
                if ( dataInfoList.size() > 0 )
                {
                    //generate documentId
                    Util.generateDocumentIdAndDatasource(dataInfoList,job,datasourceId);

                    //send back to server
                    dataService.sendbackDiscoveredObj(job.getOrganizationId(),job.getId(),job.getTaskId(),datasourceId,dataInfoList,false);
                }
            }
        }
        catch (Exception e) 
        {
            String errorInfo = String.format(" discoveData() failed! datasourceId=%d, e=%s, statcktrace=%s",datasource.getId(),e,ExceptionUtils.getStackTrace(e)); 
            log.error(errorInfo);
            
            dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);
 
            throw e;
        }
    }
    
    @Override
    protected void archiveDataobjects() throws Exception
    {   
        String url;
        boolean contentCopied;
        List<String> archiveFileList = Util.getConfigData(job.getTaskConfig(),CommonKeys.ARCHIVE_FILE_NODE);
        
        objectStorageServiceInstance = (ServiceInstance)Tool.deserializeObject(commonService.getObjStorageSI(job.getOrganizationId(),job.getTargetRepositoryId()).array());
        objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
        objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
        
        datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),job.getDatasourceId()).array());
        datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());
        
        getTaskParameters(job.getTaskConfig());  
                                
        for( String archiveFile:archiveFileList )
        {
            log.info("archive file="+archiveFile);
            
            try
            {
                contentCopied = false;
                DataobjectInfo obj = new DataobjectInfo();
                
                String dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(),job.getSourceApplicationId(),job.getDatasourceType(),job.getDatasourceId(),archiveFile,job.getTargetRepositoryId());
                
                obj.setDataobjectId(dataobjectId);
                obj.setSourceApplicationId(job.getSourceApplicationId());
                obj.setDatasourceType(job.getDatasourceType());
                obj.setDatasourceId(job.getDatasourceId());

                obj.setSourcePath("");  // set original file path
               // obj.setName(archiveFile);
              
                // read file /*
                ByteBuffer contentStream = SEUtil.readWebPageToByteBuffer(archiveFile);
                
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
      
                List<String> contentLocations = new ArrayList<>();
                                             
                if ( copyContentToSystem )
                    contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_BLOB_STORE,contentIds.get(0)));
                else
                    contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_WEB,archiveFile));
  
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
                
                FrontendMetadata metadata = new FrontendMetadata("search_keyword",true);
                metadata.setSingleValue(Util.getDatasourceProperty(datasource, "searchKeyword"));
                obj.getMetadataList().add(metadata);
                
                String charset = getUrlCharSet(archiveFile);
                String urlContentStr = getUrlContentString(archiveFile,charset);
                org.jsoup.nodes.Document doc = Jsoup.parse(urlContentStr);
                String title = doc.title(); log.info(" title="+title);
                
                obj.setName(title);
                
                metadata = new FrontendMetadata("title",true);
                metadata.setSingleValue(title);
                obj.getMetadataList().add(metadata);                
        
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
            }
            catch (Exception e)
            {
                String errorInfo = String.format(" archiveData() failed! archiveFile=%s e=%s, statcktrace=%s",archiveFile,e,ExceptionUtils.getStackTrace(e)); 
                log.error(errorInfo);
            
                dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);
                throw e;
            }
        }
        
        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.COMPLETE.getValue(),"");
    }   
       
    @Override
    protected void collectDataobjectMetadata(DataobjectInfo dataobject,String archiveFile) throws Exception
    {
        FrontendMetadata metadata;
        List<FrontendMetadata> metadataList = dataobject.getMetadataList();  
             
        metadata = new FrontendMetadata("stored_yearmonth",true);
        metadata.setSingleValue(Tool.getYearMonthStr(new Date()));
        metadataList.add(metadata);
                        
        metadata = new FrontendMetadata("url",true); 
        metadata.setSingleValue(archiveFile);
        metadataList.add(metadata);
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
        String encoding = CAUtil.detectEncoding(contentBinary);
        metadata.setSingleValue(encoding);
        metadata.setContentId(contentId);
        metadataList.add(metadata);
        
        metadata = new FrontendMetadata("mime_type",true); 
        String mime_type = "text/html";
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
            String contentText = Util.getContentTextFromBinary(contentType,contentBinary,encoding);           
            metadata.setSingleValue(contentText);
            metadata.setContentId(contentId);            
            metadataList.add(metadata);                
        }           
    }
        
    @Override
    public void generateIndexMetadata(List<Map<String,Object>> metadataDefinitions,Map<String, Object> jsonMap, List<FrontendMetadata> metadataList, Dataobject obj, Map<String,Object> contentInfo,boolean needEncryption) throws Exception 
    {
        jsonMap.put("title",Util.findSingleValueMetadataValue("title",metadataList));
        jsonMap.put("url",Util.findSingleValueMetadataValue("url",metadataList));
        jsonMap.put("search_keyword",Util.findSingleValueMetadataValue("search_keyword",metadataList));
 
        if ( contentInfo.get("contentBinaries") != null )
            jsonMap.put("contents", generateContentData(obj,contentInfo,metadataList));
    }

    @Override
    protected int getDataobjectTypeId(DataobjectInfo obj) 
    {
        return 3; // dataobject id for file in dataobject_type table , web page
    }
       
    @Override
    public boolean validateDataobject(DiscoveredDataobjectInfo obj) 
    {
        return true; 
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
    
    @Override
    protected void getConfig(DataService.Client dataService,Datasource datasource,Job job) throws Exception
    {
       // for child class to over write
    }
}