/*
 * DatabaseProcessor.java
 *
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.Element;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.enumeration.FileTypeWithStructureData;
import com.broaddata.common.model.enumeration.TargetedFileEventType;
import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.DatasourceEndTask;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataProcessingExtension;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.model.platform.StructuredFileDefinition;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.util.CommonKeys;
import com.broaddata.common.util.CommonServiceConnector;
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.PstFileUtil;
import com.broaddata.common.util.StructuredDataFileUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import java.util.HashMap;

public class RecordFileProcessor extends Processor
{
    static final Logger log = Logger.getLogger("RecordFileProcessor");
              
    private final boolean processMaxData = true;
    private boolean needToCheckRecordDuplication = true;
    private Map<String,String> objectStoreServiceProviderProperties;
        
    public RecordFileProcessor(CommonServiceConnector csConn,CommonService.Client csClient,DataServiceConnector dsConn,DataService.Client dsClient,Job newJob,DatasourceType datasourceType,ComputingNode computingNode,Counter serviceCounter)
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
      
    public RecordFileProcessor(DatasourceType datasourceType){
        this.datasourceType = datasourceType;
    }
   
    @Override
    protected void discoverDataobject() throws Exception
    {
        List<DiscoveredDataobjectInfo> dataInfoList;    
        List<String> datasourceIds;
        String UNCPath;
        String localFolder;
        String errorInfo;
        int datasourceId = 0;
        String businessDateStr = "";
        Date businessDate;
               
        datasourceIds = Util.getConfigData(job.getTaskConfig(),CommonKeys.DATASOURCE_NODE);
            
        for( String datasourceIdStr:datasourceIds )
        {
            try
            {
                datasourceId = Integer.valueOf(datasourceIdStr);
 
                datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),datasourceId).array());
                datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());
                              
                businessDateStr = job.getBusinessDate();
                businessDate = Tool.convertStringToDate(businessDateStr, "yyyy-MM-dd");
                
                //if ( businessDateStr == null || businessDateStr.trim().isEmpty() )
                //    businessDateStr = Util.getDatasourceBusinessDate(datasource);
                   
                dataInfoList = new ArrayList<>();
                
                if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                    localFolder = Tool.convertToWindowsPathFormat(Util.getSubFolder(datasourceConnection,datasource,businessDate));
                else
                    localFolder = Tool.convertToNonWindowsPathFormat(Util.mountFileServerToLocalFolder(datasourceConnection,datasource,businessDate));
                       
                FileUtil.getFolderAllFiles(localFolder,dataInfoList,this,true,true,job.getOrganizationId(),job.getId(),dataService, datasourceId, job, businessDateStr); 
          
                log.info("last remaining files="+dataInfoList.size());
                 
                if ( dataInfoList.size() > 0 )
                {
                    Util.generateDocumentIdAndDatasource(dataInfoList,job,datasourceId);
                    
                    int retry = 0;
                    
                    while(true)
                    {
                        retry ++;
                        
                        try
                        {
                           dataService.sendbackDiscoveredObj(job.getOrganizationId(),job.getId(),job.getTaskId(),datasourceId,dataInfoList, false);
                           break;
                        }
                        catch(Exception e)
                        {
                            log.error(" sendbackDiscoveryObj failed! e="+e);
                            Tool.SleepAWhileInMS(1*1000);
                            
                            if ( retry > 3 )
                                throw e;
                        }
                    }
                }
                else
                    dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.COMPLETE.getValue(), "");
            }
            catch (Exception e) 
            {
                errorInfo = String.format(" discoveData() failed! datasourceId=%d, e=%s, statcktrace=%s",datasourceId,e,ExceptionUtils.getStackTrace(e)); 
                dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);
                throw e;
            }  
        }
    }

    @Override
    protected void archiveDataobjects() throws Exception
    {
        int totalLine;
        int targetDataobjectType;
        String originalSourcePath;
        StructuredFileDefinition fileDefinition;
        Map<String,Object> importResults = null;
        InputStream fileInputStream;
        String errorInfo;
                
        List<String> archiveFileList = Util.getConfigData(job.getTaskConfig(),CommonKeys.ARCHIVE_FILE_NODE);
             
        try
        {
            objectStorageServiceInstance = (ServiceInstance)Tool.deserializeObject(commonService.getObjStorageSI(job.getOrganizationId(),job.getTargetRepositoryId()).array());
            objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
            task = (DatasourceEndTask)Tool.deserializeObject(dataService.getDatasourceEndTask(job.getOrganizationId(),job.getTaskId()).array());
         
            datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),job.getDatasourceId()).array());
            datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());
        }
        catch(Exception e)
        {
            errorInfo = " get datasource/datasourceconnection failed!  e="+e+" statcktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);

            dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);
            throw e;
        }
            
        String str = dataService.getSystemConfigValue(0, "data_collecting", "need_to_check_record_duplication");

        if ( str != null && !str.trim().isEmpty() )
        {
            try
            {
                if ( str.trim().equals("no") )
                    needToCheckRecordDuplication = false;
                else
                 if ( str.trim().equals("yes") )
                    needToCheckRecordDuplication = true;
                
                log.info("11111111111111111 MAX_CALL_ROW_NUMBER change to "+needToCheckRecordDuplication+" str="+str);
            }
            catch(Exception e)
            {
                log.error("222222222222 get new MAX_CALL_ROW_NUMBER failed! e="+e+" str="+str);
            }
        }
        else
            log.info("11111111111111111 MAX_CALL_ROW_NUMBER = "+ needToCheckRecordDuplication);
            
        getTaskParameters(job.getTaskConfig());
        
        Date businessDate = Tool.convertStringToDate(job.getBusinessDate(), "yyyy-MM-dd");
         
        if ( computingNode.getNodeOsType() != ComputingNodeOSType.Windows.getValue() )
            Util.mountFileServerToLocalFolder(datasourceConnection,datasource,businessDate);
         
        for( String archiveFile:archiveFileList )
        {
            log.info("archive file="+archiveFile);
               
            if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                originalSourcePath = archiveFile;
            else
                originalSourcePath = Util.getOriginalSourcePath(datasourceConnection,datasource,archiveFile);
                        
            String filename = FileUtil.getFileNameWithoutPath(archiveFile);
            
            if ( filename.endsWith(FileTypeWithStructureData.PST.getExt()) )
            {
                try
                {
                    targetDataobjectType = CommonKeys.DATAOBJECT_TYPE_FOR_PST_FILE;

                    Map<String,Object> parameters = new HashMap<>();
           
                    parameters.put("organizationId", job.getOrganizationId());
                    parameters.put("jobId",job.getId());
                    parameters.put("dataService",dataService);
                    parameters.put("commonService",commonService);
                    parameters.put("callback",null);
                    parameters.put("alreadyProcessedLine",job.getAlreadyProcessedLine());
                    parameters.put("datasourceType",CommonKeys.FILE_DATASOURCE_TYPE);
                    parameters.put("datasourceId",job.getDatasourceId());
                    parameters.put("sourceApplicationId",job.getSourceApplicationId());
                    parameters.put("targetRepositoryType",task.getTargetRepositoryType());
                    parameters.put("targetRepositoryId",job.getTargetRepositoryId());
                    parameters.put("targetDataobjectType",targetDataobjectType);
                    parameters.put("computingNode",computingNode);
                    parameters.put("objectStorageServiceInstance",objectStorageServiceInstance);
                    parameters.put("objectStoreServiceProviderProperties",objectStoreServiceProviderProperties);
                   
                    importResults = PstFileUtil.importPstFileToSystem(originalSourcePath,parameters);
                    
                    if ( importResults != null )
                    {
                        errorInfo = (String)importResults.get("errorInfo");
                        int status = (Integer)importResults.get("status");

                        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), status, errorInfo);
                    }
                }
                catch(Exception e)
                {
                    log.error("archiveData() failed! archiveFile="+archiveFile+" e="+e);
                }
            }
            else
            {
                try 
                {
                    fileInputStream = FileUtil.getFileInputStream(archiveFile);

                    ByteBuffer buf = dataService.getFileDefinition(job.getOrganizationId(),filename);

                    if ( buf == null )
                    {
                        errorInfo = Util.getBundleMessage("file_definition_not_found", archiveFile);
                        log.error(errorInfo);

                        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);

                        continue;
                    }
                    else
                        fileDefinition = (StructuredFileDefinition)Tool.deserializeObject(buf.array());            
                }
                catch(Exception e)
                {
                    errorInfo = " get fileDefinition failed!  filename="+filename+" e="+e;
                    log.error(errorInfo);

                    dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);
                    continue;
                }

                if ( fileDefinition.getIsEnabled() == 0 )
                {
                    log.error(" fileDefinition is not enabled!  filename="+filename);
                    errorInfo = Util.getBundleMessage("file_definition_not_found", archiveFile);

                    dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(), errorInfo);

                    continue;
                }

                if ( targetedFileEventTypeId != TargetedFileEventType.ALL_DATA.getValue() )
                {
                    if ( targetedFileEventTypeId == TargetedFileEventType.EVENT_DATA.getValue() && fileDefinition.getIsEventData() == 0 )
                    {
                        log.error("Task only processes event_time. This is not a event data! filename="+filename);
                        errorInfo = Util.getBundleMessage("file_is_not_event_data", archiveFile);
                        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.SKIPPED.getValue(), errorInfo);

                        continue;
                    }
                    else
                    if ( targetedFileEventTypeId == TargetedFileEventType.NON_EVENT_DATA.getValue() && fileDefinition.getIsEventData() == 1 )   
                    {
                        log.error("Task only processes non_event_time. This is not a non_event data! filename="+filename);
                        errorInfo = Util.getBundleMessage("file_is_not_non_event_data", archiveFile);

                        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.SKIPPED.getValue(), errorInfo);

                        continue;
                    }
                }

                try
                {
                    if ( fileDefinition.getDataProcessingExtensionId() > 0 )
                    {
                        dpe = (DataProcessingExtension)Tool.deserializeObject(dataService.getDataProcessingExtension(job.getOrganizationId(),fileDefinition.getDataProcessingExtensionId()).array());   
                        dataProcessingExtension = Util.getDataProcessingExtensionClass(dpe.getProcessingExtensionClass(),dataService,datasource,job);
                    }
                    else
                        dataProcessingExtension = null;
                }
                catch(Exception e)
                {
                    dataProcessingExtension = null;
                }

                try
                {
                    if ( fileDefinition.getFileType() == FileTypeWithStructureData.TXT.getValue() || fileDefinition.getFileType() == FileTypeWithStructureData.DEL.getValue())
                    {
                        boolean removeAroundQuotation = fileDefinition.getFileType() == FileTypeWithStructureData.DEL.getValue();
                        importResults = StructuredDataFileUtil.importTxtFileToSystem(archiveFile,false,job.getOrganizationId(),job.getId(),dataValidationBehaviorTypeId,fileInputStream,fileDefinition.getProcessStartRow(),job.getProcessStartLine(),processMaxData,job.getProcessRowNumber(),dataService,commonService,fileDefinition,CommonKeys.FILE_DATASOURCE_TYPE, job.getDatasourceId(),job.getSourceApplicationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),removeAroundQuotation,null,dataProcessingExtension,job.getAlreadyProcessedLine(),needToCheckRecordDuplication,serviceCounter);
                    }
                    else
                    if ( fileDefinition.getFileType() == FileTypeWithStructureData.EXCEL.getValue() )
                        importResults = StructuredDataFileUtil.importExcelFileToSystem(false,job.getOrganizationId(),job.getId(),dataValidationBehaviorTypeId,filename,fileInputStream,fileDefinition.getProcessStartRow(),job.getProcessStartLine(), processMaxData,job.getProcessRowNumber(),dataService,commonService,fileDefinition,CommonKeys.FILE_DATASOURCE_TYPE, job.getDatasourceId(),job.getSourceApplicationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),null,dataProcessingExtension,job.getAlreadyProcessedLine(),needToCheckRecordDuplication,serviceCounter);
          
                    if ( importResults != null )
                    {
                        errorInfo = (String)importResults.get("errorInfo");
                        int status = (Integer)importResults.get("status");

                        dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), status, errorInfo);
                    }
                }
                catch (Exception e)
                {
                    log.error("archiveData() failed! archiveFile="+archiveFile+" e="+e+", stacktrace="+ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }
                
    private void getTaskParameters(String taskConfig) 
    {    
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
                    case "dataValidationBehaviorType":
                        //dataValidationBehaviorTypeId = Integer.parseInt(value);
                        dataValidationBehaviorTypeId = 1;
                        
                    case "targetedFileEventType":
                        targetedFileEventTypeId = Integer.parseInt(value);
                        break;
                }
            }
        }
        catch (Exception e) {
             throw e;
        }
    }
     
    private static Map<String,Object> getColumnDefinition(List<Map<String, Object>> columnDefinitions, int columnNo) 
    {
         for(Map<String,Object> columnDefinition : columnDefinitions)
         {
             String id = (String)columnDefinition.get("id");
             
             if ( columnNo == Integer.parseInt(id) )
                 return columnDefinition;
         }
         
         return null;
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
    }
    
    @Override
    protected void beforeStoringToServer(DataobjectInfo dataobject) throws Exception 
    {
        return; // for sub-class to override
    }
    
    @Override
    protected int getDataobjectTypeId(DataobjectInfo obj) {
         throw new UnsupportedOperationException("Not supported yet."); // no need for recordfile processor
    }

    @Override
    public boolean validateDataobject(DiscoveredDataobjectInfo obj) {
        return true;
    }
    
    @Override
    protected void collectDataobjectContentMetadata(DataService.Client dataService,Datasource datasource,Job job, DataobjectInfo dataobject, List<String> contentIds, List<ByteBuffer> contentBinaries, List<String> contentLocations, List<Boolean> needToExtractContentTexts,String archiveFile) throws Exception {
       throw new UnsupportedOperationException("Not supported yet."); // no need for recordfile processor
    }   

    @Override
    public void generateIndexMetadata(List<Map<String, Object>> metadataDefinitions, Map<String, Object> jsonMap, List<FrontendMetadata> metadataList, Dataobject obj, Map<String, Object> contentInfo,boolean needEncryption) throws Exception {
        generateIndexMetadataDefault(metadataDefinitions,jsonMap,metadataList,obj,contentInfo,needEncryption);
        
        if ( contentInfo.get("contentBinaries") != null )
            jsonMap.put("contents", generateContentData(obj,contentInfo,metadataList));
    }
    
    @Override
    protected void getConfig(DataService.Client dataService,Datasource datasource,Job job) throws Exception
    {
       // for child class to over write
    }
}