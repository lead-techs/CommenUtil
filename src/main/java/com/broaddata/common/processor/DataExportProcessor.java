/*
 * DataExportProcessor.java
 *
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
  
import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.enumeration.DataExportStyle;
import com.broaddata.common.model.enumeration.DataExportType;
import com.broaddata.common.model.enumeration.FileTypeForExport;
import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.AnalyticQuery;
import com.broaddata.common.model.organization.DataExportTask;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.ExportJob;
import com.broaddata.common.util.CommonKeys;
import com.broaddata.common.util.DataExportUtil;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.MailUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;

public class DataExportProcessor
{
    static final Logger log = Logger.getLogger("DataExportProcessor");
    
    public static void process(DataService.Client dataService,ExportJob exportJob, ComputingNode computingNode) throws Exception
    {
        DataExportTask dataExportTask = (DataExportTask)Tool.deserializeObject(exportJob.getDataExportTask());
        
        DataExportType exportType = DataExportType.findByValue(dataExportTask.getExportType());
        
        switch(exportType)
        {
            case FROM_DATAOBJECT_TYPE:
                processDataobjectType(dataService,exportJob,computingNode,dataExportTask);
                break;
            case FROM_DATAVIEW:
                processDataview(dataService,exportJob,computingNode,dataExportTask);
                break;
        }   
    }
      
    public static void processDataobjectType(DataService.Client dataService,ExportJob exportJob,ComputingNode computingNode,DataExportTask dataExportTask) throws Exception
    {
        int comparingFieldType = 0;
        String comparingField = "";
        DatasourceConnection sourceDatasourceConnection;
        DatasourceConnection targetDatasourceConnection;
 
        try
        {
            log.info("11111111111111 dataExportTask organizationId="+dataExportTask.getOrganizationId());
                   
            if ( DataExportStyle.findByValue(dataExportTask.getExportStyle()) == DataExportStyle.STORE_TO_DATABASE )
            {
                sourceDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getRepositorySQLdbDatasourceConnection(dataExportTask.getOrganizationId(),dataExportTask.getRepositoryId()).array());
                targetDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(dataExportTask.getOrganizationId(),dataExportTask.getExportDatasource()).array());
                          
                String filterStr = dataExportTask.getFilterString();
            
                DataobjectType selectedDataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(dataExportTask.getDataobjectTypeId()).array());
                
                List<String> tableColumns = new ArrayList<>();
             
                List<Map<String, Object>> metadataInfoList = (List<Map<String, Object>>)Tool.deserializeObject(dataService.getDataobjectTypeMetadataDefinition(dataExportTask.getDataobjectTypeId(),true, true).array());
                
                for(Map<String,Object> definition : metadataInfoList)
                {
                    String name = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                    String description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    tableColumns.add(name+"-"+description);
                    
                    if ( dataExportTask.getComparingField() != null && !dataExportTask.getComparingField().trim().isEmpty() )
                    {
                        String fieldName = dataExportTask.getComparingField();
                        
                        if ( fieldName.contains("-") )
                            fieldName = fieldName.substring(0, fieldName.indexOf("-"));
                        
                        String newName = Tool.changeTableColumnName(selectedDataobjectType.getName(),name,true);
                        
                        if ( newName.trim().toUpperCase().equals(fieldName.trim().toUpperCase()) )
                        {
                            comparingFieldType = Integer.parseInt((String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
                            comparingField = fieldName;
                        }
                    }
                }
                
                log.info("1111111111 comparingFieldType="+comparingFieldType+" comparingField="+comparingField);
              
                String errorInfo = DataExportUtil.saveToDatabaseFromSqldb(exportJob.jobId,dataService,dataExportTask.getOrganizationId(),sourceDatasourceConnection,targetDatasourceConnection,dataExportTask.getExportTableName(),tableColumns,selectedDataobjectType,metadataInfoList,dataExportTask.getDatabaseUpdateMode(),10000,dataExportTask.getRepositoryId(),dataExportTask.getCatalogId(),filterStr,"",true,dataExportTask.getLastProcessedItem(),comparingFieldType,comparingField);
                    
                if ( errorInfo != null && !errorInfo.isEmpty() )
                    throw new Exception("processDataobjectType() failed! errorInfo="+errorInfo);
            }
            else
                throw new Exception("unsupported export style!");
                      
            while(true)
            {
                try
                {
                    dataService.updateClientJobStatus(ClientJobType.DATA_EXPORT_JOB.getValue(),dataExportTask.getOrganizationId(),exportJob.getJobId(),TaskJobStatus.COMPLETE.getValue(), "");
                    break;
                }
                catch(Exception e)
                {
                    log.error("dataService.updateClientJobStatus() failed! e="+e);
                    Tool.SleepAWhile(1, 0);
                }
            }
        }
        catch (Exception e) 
        {
            String errorInfo = "process data export job failed!  exception="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
            
            log.info(" dataExportTask organizationId="+dataExportTask.getOrganizationId());
            
            dataService.updateClientJobStatus(ClientJobType.DATA_EXPORT_JOB.getValue(),dataExportTask.getOrganizationId(),exportJob.getJobId(),TaskJobStatus.FAILED.getValue(),errorInfo);

            throw e;
        }
    }
    
    public static void processDataobjectTypeFromES(DataService.Client dataService,ExportJob exportJob,ComputingNode computingNode,DataExportTask dataExportTask) throws Exception
    {
        DatasourceConnection datasourceConnection;
 
        try
        {
            log.info("11111111111111 dataExportTask organizationId="+dataExportTask.getOrganizationId());
                   
            if ( DataExportStyle.findByValue(dataExportTask.getExportStyle()) == DataExportStyle.STORE_TO_DATABASE )
            {
                datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(dataExportTask.getOrganizationId(),dataExportTask.getExportDatasource()).array());
                          
                String filterStr = dataExportTask.getFilterString();
            
                DataobjectType selectedDataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(dataExportTask.getDataobjectTypeId()).array());
                
                List<String> tableColumns = new ArrayList<>();
             
                List<Map<String, Object>> metadataInfoList = (List<Map<String, Object>>)Tool.deserializeObject(dataService.getDataobjectTypeMetadataDefinition(dataExportTask.getDataobjectTypeId(),true, true).array());
                
                for(Map<String,Object> definition : metadataInfoList)
                {
                    String name = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                    String description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    tableColumns.add(name+"-"+description);
                }
              
                String errorInfo = DataExportUtil.saveToDatabaseFromES(exportJob.jobId,dataService,dataExportTask.getOrganizationId(),datasourceConnection,dataExportTask.getExportTableName(),tableColumns,selectedDataobjectType,metadataInfoList,dataExportTask.getDatabaseUpdateMode(),10000,dataExportTask.getRepositoryId(),dataExportTask.getCatalogId(),filterStr,"",true);
                    
                if ( errorInfo != null && !errorInfo.isEmpty() )
                    throw new Exception("processDataobjectType() failed! errorInfo="+errorInfo);
            }
            else
                throw new Exception("unsupported export style!");
                      
            while(true)
            {
                try
                {
                    dataService.updateClientJobStatus(ClientJobType.DATA_EXPORT_JOB.getValue(),dataExportTask.getOrganizationId(),exportJob.getJobId(),TaskJobStatus.COMPLETE.getValue(), "");
                    break;
                }
                catch(Exception e)
                {
                    log.error("dataService.updateClientJobStatus() failed! e="+e);
                    Tool.SleepAWhile(1, 0);
                }
            }
        }
        catch (Exception e) 
        {
            String errorInfo = "process data export job failed!  exception="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
            
            log.info(" dataExportTask organizationId="+dataExportTask.getOrganizationId());
            
            dataService.updateClientJobStatus(ClientJobType.DATA_EXPORT_JOB.getValue(),dataExportTask.getOrganizationId(),exportJob.getJobId(),TaskJobStatus.FAILED.getValue(),errorInfo);

            throw e;
        }
    }
      
    public static void processDataview(DataService.Client dataService,ExportJob exportJob, ComputingNode computingNode,DataExportTask dataExportTask) throws Exception
    {
        String filename = null;
        String newFileName;
        String localFolder;
        String tmpDirectory;
        Datasource datasource;
        DatasourceConnection datasourceConnection;
        AnalyticDataview dataview;
        AnalyticQuery query;
        
        try
        {
            log.info("11111111111111 dataExportTask organizationId="+dataExportTask.getOrganizationId());
            
            dataview = (AnalyticDataview)Tool.deserializeObject(exportJob.getDataview());
            
            //query = (AnalyticQuery)Tool.deserializeObject(exportJob.getQuery());

            if ( DataExportStyle.findByValue(dataExportTask.getExportStyle()) == DataExportStyle.COPY_TO_FOLDER || 
                    DataExportStyle.findByValue(dataExportTask.getExportStyle()) == DataExportStyle.SEND_FILE_TO_EMAIL )// || 
                   // DataExportStyle.findByValue(dataExportTask.getExportStyle()) == DataExportStyle.SEND_FILE_LINK_TO_EMAIL )
            {
                tmpDirectory = Tool.getSystemTmpdir();
                
                switch(FileTypeForExport.findByValue(dataExportTask.getExportFileType()))
                {
                    case CSV:
                        filename = DataExportUtil.exportDataToTxt(dataService,dataExportTask,exportJob,dataview,tmpDirectory);
                        break;
                    case EXCEL:
                        filename = DataExportUtil.exportDataToExcel(dataService,dataExportTask,exportJob,dataview,tmpDirectory);
                        break;
                }
                
                if ( DataExportStyle.findByValue(dataExportTask.getExportStyle()) == DataExportStyle.COPY_TO_FOLDER )
                {
                    datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(dataExportTask.getOrganizationId(),dataExportTask.getExportDatasource()).array());
                    datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(dataExportTask.getOrganizationId(),datasource.getDatasourceConnectionId()).array());
                
                    if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                        localFolder = Tool.convertToWindowsPathFormat(Util.getSubFolder(datasourceConnection,datasource,null));
                    else
                        localFolder = Tool.convertToNonWindowsPathFormat(Util.mountFileServerToLocalFolder(datasourceConnection,datasource,null));  
                    
                    newFileName = Tool.getDataExportFileName(filename);
                    FileUtil.copyFileToFolder(filename,localFolder,newFileName);
                }
                else // send file to email
                if ( DataExportStyle.findByValue(dataExportTask.getExportStyle()) == DataExportStyle.SEND_FILE_TO_EMAIL )
                {
                    String shortFilename = filename.substring(filename.indexOf(".")+1);
                    String mailSubject = Util.getBundleMessage("data_subscription")+":"+dataExportTask.getName()+" "+Tool.convertDateToString(new Date(), "yyyy-MM-dd hh:mm:ss");
                    String mailBody = Util.getBundleMessage("data_subscription")+":"+dataExportTask.getName()+" "+Util.getBundleMessage("file_name")+":"+shortFilename;
                    
                    String fromMailBox = dataService.getSystemConfigValue(dataExportTask.getOrganizationId(),"email", "from_mailbox");
                    String fromMailBoxUsername = dataService.getSystemConfigValue(dataExportTask.getOrganizationId(),"email", "from_mailbox_user_name");
                    String fromMailBoxPassword = dataService.getSystemConfigValue(dataExportTask.getOrganizationId(),"email", "from_mailbox_user_password");
                    
                    List<String> attachements = new ArrayList<>();
                    attachements.add(filename);
                                        
                    List<Map<String, String>> list = dataService.getDataSubscriberInfo(dataExportTask.getOrganizationId(), dataExportTask.getId());
                     
                    for(Map<String,String> map : list)
                    {
                        String sendToMailbox = map.get("email");
                        
                        log.info(" sending to %s"+sendToMailbox);
                        MailUtil.sendMail(sendToMailbox, fromMailBox, fromMailBoxUsername, fromMailBoxPassword,mailSubject, mailBody, attachements);
                    }
                }
            }
            else  // save to database
            {
                datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(dataExportTask.getOrganizationId(),dataExportTask.getExportDatasource()).array());

                DataExportUtil.exportDataviewDataToDatabase(dataService,dataExportTask,exportJob,dataview,datasourceConnection);
            }
                     
            dataService.updateClientJobStatus(ClientJobType.DATA_EXPORT_JOB.getValue(),dataExportTask.getOrganizationId(),exportJob.getJobId(),TaskJobStatus.COMPLETE.getValue(), "");
        }
        catch (Exception e) 
        {
            String errorInfo = "process data export job failed!  exception="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
            
            log.info(" dataExportTask organizationId="+dataExportTask.getOrganizationId());
            
            dataService.updateClientJobStatus(ClientJobType.DATA_EXPORT_JOB.getValue(),dataExportTask.getOrganizationId(),exportJob.getJobId(),TaskJobStatus.FAILED.getValue(),errorInfo);

            throw e;
        }
    }
}