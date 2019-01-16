/*
 * DatabaseProcessor.java
 *
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.transport.TTransportException;

import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.enumeration.DataValidationBehaviorType;
import com.broaddata.common.model.enumeration.DatabaseArchiveMethodType;
import com.broaddata.common.model.enumeration.DatabaseItemType;
import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.enumeration.DatasourceEndTaskType;
import com.broaddata.common.model.enumeration.JobItemProcessingStatisticType;
import com.broaddata.common.model.enumeration.JobTimeType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.TargetRepositoryType;
import com.broaddata.common.model.enumeration.TaskJobStatus;
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
import com.broaddata.common.util.CAUtil;
import com.broaddata.common.util.CommonKeys;
import com.broaddata.common.util.CommonServiceConnector;
import com.broaddata.common.util.ContentStoreUtil;
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.JDBCUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import java.io.IOException;

public class DatabaseProcessor extends Processor
{
    static final Logger log = Logger.getLogger("DatabaseProcessor");
       
    static final int FETCH_SIZE = 1000;
    static int MAX_CALL_ROW_NUMBER = 20000;
   
    private boolean isPagenated;
    private String querySQL;
    private String countQuerySQL;
    private int count;
    private String minValue,maxValue;
    private Date minDate,maxDate;
          
    private DatasourceConnection targetDatasourceConnection = null;
    private int targetDataobjectTypeId;
    private int databaseItemType;
    private String databaseItem;
    private boolean isChangeableData = false;
    private boolean needToProcessComparingFieldNullValueData = false;
    private boolean invokingDataServiceStyle = false;
    private int databaseArchiveMethodType;
    private int comparingFieldType;    
    private String comparingFieldName;
    private String comparingFieldDataType;
    private String comparingFieldValue;
    private Date comparingFieldValueDate;    
    private String primaryKeyStr;
    private String schema;

    private List<Map<String,Object>> databaseItemDefinitions;
    private Map<String,Object> definition;
    
    private String fieldValue;
    private String newFieldValue;
    private String dbDriver;
    private Connection dbConn = null;
    private PreparedStatement statement = null;
    private ResultSet resultSet;
    private FrontendMetadata metadata;
    private List<FrontendMetadata> metadataList;
    private String contentId;
    private String contentName;
    private List<String> contentIds;
    private List<String> contentNames;
    private List<String> contentLocations;
    private List<Boolean> needToExtractContentTexts;
    private List<Boolean> needToSaveContentInSystems;
    private ByteBuffer contentBinary;
    private List<ByteBuffer> contentBinaries;
    private String objectStoreServiceProvider;
    private Map<String,String> objectStoreServiceProviderProperties;        
    private boolean needToSaveContentInSystem = true;
    private String primaryKeyValue = null;
    private DataobjectInfo obj = null;
    private String dataobjectNameStr = null;
    private long totalSize;
    private String changeTableName;
    private String filter;
    private Date archiveStartTime;
    private String lastProcessedItem;
    private String lastProcessedItemInDB;
    private String lastProcessedItemPlus;
    private String columnNameListStr;
    private DatabaseType databaseType;
    private SqldbWriter sqldbWriter;
    private int lastProcessedItemDelay = 0;
     
    public DatabaseProcessor(CommonServiceConnector csConn,CommonService.Client csClient,DataServiceConnector dsConn,DataService.Client dsClient,Job newJob,DatasourceType datasourceType,ComputingNode computingNode,Counter serviceCounter)
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
     
    public DatabaseProcessor(DatasourceType datasourceType){
        this.datasourceType = datasourceType;
    }
     
    @Override
    protected void beforeStoringToServer(DataobjectInfo dataobject) throws Exception 
    {
        return; // for sub-class to override
    }
   
    @Override
    protected void discoverDataobject() throws Exception{
    }

    @Override
    protected void archiveDataobjects() throws Exception
    {
        String errorInfo = "";
        
        try
        {
            datasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),job.getDatasourceId()).array());                
            datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),datasource.getDatasourceConnectionId()).array());
           
            task = (DatasourceEndTask)Tool.deserializeObject(dataService.getDatasourceEndTask(job.getOrganizationId(),job.getTaskId()).array());
         
            if ( task.getTargetRepositoryType() == TargetRepositoryType.DATA_REPOSITORY.getValue() )
            {
                objectStorageServiceInstance = (ServiceInstance)Tool.deserializeObject(commonService.getObjStorageSI(job.getOrganizationId(),job.getTargetRepositoryId()).array());
                objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
                objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
            }
                                    
            String str = dataService.getSystemConfigValue(0, "data_collecting", "db_max_row");
            
            if ( str != null && !str.trim().isEmpty() )
            {
                try
                {
                    MAX_CALL_ROW_NUMBER = Integer.parseInt(str);
                    log.info("11111111111111111 MAX_CALL_ROW_NUMBER change to "+str);
                }
                catch(Exception e)
                {
                    log.error("222222222222 get new MAX_CALL_ROW_NUMBER failed! e="+e+" str="+str);
                }
            }
            else
                log.info("11111111111111111 MAX_CALL_ROW_NUMBER = "+ MAX_CALL_ROW_NUMBER);
              
            str = dataService.getSystemConfigValue(0, "data_collecting", "db_max_row_for_rowid");
                        
            getTaskParameters(task.getConfig());
            
            String databaseTypeStr = Util.getDatasourceConnectionProperty(datasourceConnection, "database_type");
            
            databaseType = DatabaseType.findByValue(Integer.parseInt(databaseTypeStr));
            
            schema = Util.getDatabaseSchema(datasourceConnection);

            if ( databaseType == DatabaseType.SQLITE )
            {
                String sqliteFileDatasourceIdStr = Util.getDatasourceConnectionProperty(datasourceConnection, "location");
                
                if ( !(sqliteFileDatasourceIdStr == null || sqliteFileDatasourceIdStr.trim().isEmpty()) )  // has datasource location, replace
                {
                    String dbUrl = datasourceConnection.getLocation(); // jdbc:sqlite:testdb
                    
                    if ( computingNode.getNodeOsType() != ComputingNodeOSType.Windows.getValue() )
                    {
                        dbUrl = generateNewDbUrlForLinux(dbUrl,sqliteFileDatasourceIdStr);
                        datasourceConnection.setLocation(Tool.convertToNonWindowsPathFormat(dbUrl));
                    }
                    else  // is windows 
                    {
                        dbUrl = generateNewDbUrlForWindows(dbUrl,sqliteFileDatasourceIdStr);
                        datasourceConnection.setLocation(Tool.convertToWindowsPathFormat(dbUrl));
                    }                 
                }
            }
 
            getDatasourceConfig(datasource.getProperties());
            
            log.info(" dataProcessingExtensionId = "+ dataProcessingExtensionId);
            
            try
            {
                if ( dataProcessingExtensionId > 0 )
                {
                    dpe = (DataProcessingExtension)Tool.deserializeObject(dataService.getDataProcessingExtension(job.getOrganizationId(),dataProcessingExtensionId).array());    
                    dataProcessingExtension = Util.getDataProcessingExtensionClass(dpe.getName(),dataService,datasource,job);
                }
                else
                    dataProcessingExtension = null;                
            }
            catch(Exception e)
            {
                 dataProcessingExtension = null;
            }
            
            //log.info("1111111111111111111111111");
            
            columnNameListStr = getColumnNameListStr();           

            //log.info("1111111111111111111111111 columnNameListStr="+columnNameListStr);
            
            dbConn = JDBCUtil.getJdbcConnection(datasourceConnection);
   
            log.info("dbConn = "+dbConn);

            if ( job.getTaskType() == DatasourceEndTaskType.FULL_ARCHIVE.getValue() )
            {
                targetDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());           
 
                FullArchivingProcess fullArchivingProcess = new FullArchivingProcess (job, dataService, computingNode, datasourceConnection, databaseType, schema, databaseItem, databaseItemType, filter, targetDatasourceConnection, targetDataobjectTypeId);
                errorInfo = fullArchivingProcess.processFullArchiving();
            }
            else
            {
                log.info("1111 databaseArchiveMethodType="+databaseArchiveMethodType+" comparingFieldName="+comparingFieldName);
                
                switch(  DatabaseArchiveMethodType.findByValue(databaseArchiveMethodType) )
                {
                    case SYNC_BY_UPDATED_TABLE_FIELD:
                        if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() || comparingFieldType == MetadataDataType.DATE.getValue() || 
                                comparingFieldType == MetadataDataType.INTEGER.getValue() || comparingFieldType == MetadataDataType.LONG.getValue() ||
                                comparingFieldType == MetadataDataType.FLOAT.getValue() || comparingFieldType == MetadataDataType.DOUBLE.getValue() )
                        {
                            if ( needToProcessComparingFieldNullValueData )
                            {
                                String additionalFilter = String.format("%s is null",comparingFieldName);
                                errorInfo = processFullSync(additionalFilter);

                                if ( !errorInfo.isEmpty() )
                                     log.error(" process processFullSync() failed for additionalFilter="+additionalFilter);
                            }
                            
                            errorInfo = processOnlyNewRecord();
                        }
                        else //
                        if ( comparingFieldType == MetadataDataType.STRING.getValue() )
                        {
                            if ( needToProcessComparingFieldNullValueData )
                            {
                                String additionalFilter = String.format("(%s is null or %s='')",comparingFieldName,comparingFieldName);

                                errorInfo = processFullSync(additionalFilter);

                                if ( !errorInfo.isEmpty() )
                                     log.error(" process processFullSync() failed for additionalFilter="+additionalFilter);
                            }
                              
                            errorInfo = processOnlyNewRecordForString(); // string field
                        }
                        
                        break;
                    //case SYNC_BY_CHANGE_TABLE:
                    //    errorInfo = processChangeTable();
                    //    break;
                    case SYNC_BY_DETECTING_CONTENT_CHANGE:
                        errorInfo = processFullSync("");
                        break;
                    //case SYNC_BY_ROWID:
                    //    errorInfo = processByRowid();
                    //    break;
                }
            }

            if ( errorInfo.isEmpty() )
                dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.COMPLETE.getValue(),"");
            else                         
                dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(),errorInfo);
        }
        catch (Throwable t)
        {
            log.error("archiveData() failed! e="+t.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(t));          
            dataService.updateClientJobStatus(ClientJobType.DATASOURCE_END_JOB.getValue(),job.getOrganizationId(), job.getId(), TaskJobStatus.FAILED.getValue(),t.getMessage());
        }
        finally
        {            
            if ( dbConn!=null )
                JDBCUtil.close(dbConn);
            
            try
            {
                if ( sqldbWriter != null )
                    sqldbWriter.quitThread();
            }
            catch(Exception ee) {
            }
        }        
    }
        
    private String processFullArchiving() throws Exception
    {
        FullArchivingProcess fullArchivingProcess = new FullArchivingProcess (job, dataService, computingNode, datasourceConnection, databaseType, schema, databaseItem, databaseItemType, filter, targetDatasourceConnection, targetDataobjectTypeId);
        return fullArchivingProcess.processFullArchiving();
    }

    private String generateNewDbUrlForLinux(String dbUrl, String sqliteFileDatasourceIdStr) 
    {
        try 
        {
            Datasource sqliteFileLocationDatasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),Integer.parseInt(sqliteFileDatasourceIdStr)).array());                
            DatasourceConnection sqliteFileLocationDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),sqliteFileLocationDatasource.getDatasourceConnectionId()).array());

            String path = Util.mountFileServerToLocalFolder(sqliteFileLocationDatasourceConnection,sqliteFileLocationDatasource,null);
            String dbName = dbUrl.substring(CommonKeys.SQLITE_URL_PREFIX.length());
            
            return String.format("%s%s/%s",CommonKeys.SQLITE_URL_PREFIX,path,dbName);
        }
        catch(Exception e)
        {
            log.error("generateNewDbUrl() failed! e="+e);
            return null;
        }
    }
    
    private String generateNewDbUrlForWindows(String dbUrl, String sqliteFileDatasourceIdStr) 
    {
        try 
        {
            Datasource sqliteFileLocationDatasource = (Datasource)Tool.deserializeObject(dataService.getDatasource(job.getOrganizationId(),Integer.parseInt(sqliteFileDatasourceIdStr)).array());                
            DatasourceConnection sqliteFileLocationDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),sqliteFileLocationDatasource.getDatasourceConnectionId()).array());
            
            String path = Util.getSubFolder(sqliteFileLocationDatasourceConnection,sqliteFileLocationDatasource,null);
 
            String dbName = dbUrl.substring(CommonKeys.SQLITE_URL_PREFIX.length());
            
            return String.format("%s%s/%s",CommonKeys.SQLITE_URL_PREFIX,path,dbName);
        }
        catch(Exception e)
        {
            log.error("generateNewDbUrlForWindows() failed! e="+e);
            return null;
        }
    }
    
    private void getQueryCount(String countQuerySQL) throws Exception
    {
        try 
        {
            log.info(" countQuerySQL ="+countQuerySQL);
            
            statement = dbConn.prepareStatement(countQuerySQL);
            resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();
 
            count = resultSet.getInt(1);
            
            if ( comparingFieldType == MetadataDataType.DATE.getValue() || comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                minDate = resultSet.getTimestamp(2);
                maxDate = resultSet.getTimestamp(3);
            }
            else
            {
                minValue = resultSet.getString(2);
                maxValue = resultSet.getString(3);
            } 
                        
            log.info(" count="+count+" minValue="+minValue+" maxValue="+maxValue+" minDate="+minDate+" maxDate="+maxDate);
        }
        catch(Exception e)
        {
            log.info("getQueryCount() failed! e="+e);
            throw e;
        }
        finally
        {
            try {                  
                resultSet.close();
                statement.close();
            }
            catch(Exception e) {
            }
        }
    }
    
    private void getQueryCountOnly(String countQuerySQL) throws Exception
    {
        try 
        {
            log.info(" countQuerySQL ="+countQuerySQL);
            
            statement = dbConn.prepareStatement(countQuerySQL);
            resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();
 
            count = resultSet.getInt(1);             
            
            log.info(" count="+count);
        }
        catch(Exception e)
        {
            log.info("getQueryCount() failed! e="+e);
            throw e;
        }
    }
    
    private String getStartTime()
    {
        if ( task.getLastProcessedItem() == null || task.getLastProcessedItem().trim().isEmpty() )
            return Tool.convertDateToTimestampString(archiveStartTime);
        else
            return task.getLastProcessedItem();
    }
 
    private String processFullSync(String additionalFilter) throws Exception
    {
        int processSucceedCount = 0;
        int skippedCount = 0;
        String dataobjectId = null;
        String recordChecksumStr = null;
        String recordChecksum = null;
        Date startTime1 = new Date();
        StringBuilder errorInfo = new StringBuilder();
        List<DataobjectInfo> dataobjectInfoList;
  
        long startLine = 1;
        int retry = 0;
        int pageSize = 50000;
        Map<String, String> dataobjectIdChecksumMap = new HashMap<>();
        Map<String,Boolean> checksumChangedMap;
        String newFilter = "";
       
        try
        {      
            log.info("processFullSync() ...additionalFilter="+additionalFilter);
        
            newFilter = filter;
            
            if ( additionalFilter != null && !additionalFilter.trim().isEmpty() )
            {
                if ( newFilter == null || newFilter.trim().isEmpty() )
                    newFilter = additionalFilter;
                else
                    newFilter = String.format("%s and %s",filter,additionalFilter);
            }
            
            countQuerySQL = generateQuerySQLForFullSyncCount(newFilter);
            getQueryCountOnly(countQuerySQL);
            
            if ( additionalFilter != null && !additionalFilter.trim().isEmpty() && count == 0 )
            {
                log.info("111 count==0, not result, return");
                return "";
            }
                        
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), count);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 0);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), 0);

            if ( count == 0 )
            {
                log.info("222 count==0, not result, return");
                return "";
            }
             
            int page = 0;
            
            while(true)
            {
                page++;
                startLine = (page-1)*pageSize + 1;
                 
                log.info("111111111111111111111 startLine="+startLine+" pagesize="+pageSize);
                
                querySQL = generateQuerySQLForFullSync(startLine,pageSize,newFilter);
            
                log.info(String.format("executing querySql = %s ",querySQL));

                statement = dbConn.prepareStatement(querySQL);
                statement.setFetchSize(FETCH_SIZE);

                resultSet = JDBCUtil.executeQuery(statement);

                log.info(String.format(" finish executing!!!!! "));

                dataobjectInfoList = new ArrayList<>();
                
                if ( resultSet.next() == false )
                {
                    log.info(" no more result!");
                    break;
                }
                else
                {
                    do
                    {
                        primaryKeyValue = getPrimaryKeyValue(primaryKeyStr,resultSet,columnNameListStr);

                        //recordChecksumStr = getRecordChecksumStr(columnNameListStr, resultSet);

                        //if ( recordChecksumStr == null || recordChecksumStr.trim().isEmpty() )
                        //    continue;

                        dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(),job.getSourceApplicationId(),job.getDatasourceType(),job.getDatasourceId(),primaryKeyValue,job.getTargetRepositoryId());

                        //recordChecksum = DataIdentifier.generateDataobjectChecksum(job.getOrganizationId(), dataobjectId, recordChecksumStr);
                        //log.info("dataobjectId="+dataobjectId+" checksumstr="+recordChecksumStr+" checksum"+recordChecksum);

                        //dataobjectIdChecksumMap.put(dataobjectId,recordChecksum);
                        
                        // changed or not exisit
                        log.info("processFullSync(): process single record... key="+primaryKeyValue);

                        try
                        {
                            processSingleRecord(dataobjectId,"",dataobjectInfoList);
                            processSucceedCount++;
                        }
                        catch(Exception e)
                        {                   
                            log.error("processFullSync() processSingleRecord() failed! e="+e+" dataojbectId="+dataobjectId+" stacktrace="+ExceptionUtils.getStackTrace(e));
                            errorInfo.append(String.format("processSingleRecord() failed! primaryKeyValue=%s,errorInfoForEachRecord=(%s) stacktrace=(%s)\n\n", comparingFieldName,lastProcessedItem,e.getMessage(),ExceptionUtils.getStackTrace(e)));

                            if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue() )
                                 break;

                            dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);
                        }
                    }
                    while(resultSet.next());   
                    
                    if ( processSucceedCount > 0 )
                    {
                        Date startTime = new Date();

                        if ( task.getTargetRepositoryType() == TargetRepositoryType.DATA_REPOSITORY.getValue() )
                        {
                            retry = 0;
                            while(true)
                            {
                                retry ++;

                                try
                                {
                                    //checksumChangedMap = dataService.isDataobjectChecksumChanged(job.getOrganizationId(), job.getTargetRepositoryId(), targetDataobjectTypeId, dataobjectIdChecksumMap);
                                    checksumChangedMap = new HashMap<>();
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.storeDataobjects failed! e="+e);

                                    if ( e instanceof TTransportException || dataService == null )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.isDataobjectChecksumChanged failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }                          

                            log.info("check checksum change, took ["+(new Date().getTime()-startTime.getTime())+"] ms");

                            List<DataobjectInfo> newDataobjectInfoList = new ArrayList<>();

                            for(DataobjectInfo dInfo : dataobjectInfoList )
                            {
                                Boolean isChanged = checksumChangedMap.get(dInfo.getDataobjectId());

                                isChanged = true;
                                
                                if ( !isChanged ) // no changed
                                {
                                    log.info("skip, no change for dataobject id="+dataobjectId+" checksum="+recordChecksum+" skippedCount="+skippedCount);
                                    skippedCount++;

                                    if ( skippedCount >= 500 )
                                    {
                                        dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(),skippedCount);
                                        skippedCount = 0;
                                    }

                                }
                                else
                                    newDataobjectInfoList.add(dInfo);   
                            }

                            dataobjectInfoList = newDataobjectInfoList;
                            
                        }
                       
                        if ( dataobjectInfoList.size() > 0 )
                        {
                            retry = 0;

                            while(true)
                            {
                                retry ++;

                                try
                                {
                                    if ( task.getTargetRepositoryType() == TargetRepositoryType.SQL_DB.getValue() )
                                    {
                                        if ( sqldbWriter == null )
                                        {
                                            sqldbWriter = new SqldbWriter();

                                            dataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(targetDataobjectTypeId).array());

                                            List<Map<String, Object>> metadataDefinitions = (List<Map<String, Object>>)Tool.deserializeObject(dataService.getDataobjectTypeMetadataDefinition(targetDataobjectTypeId, false, true).array());

                                            DatasourceConnection datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());

                                            sqldbWriter.setConfig(task.getOrganizationId(), job.getTargetRepositoryId(),dataobjectType.getName(),dataobjectType,metadataDefinitions,datasourceConnection);
                                        }

                                        sqldbWriter.processData(dataobjectInfoList);
                                    }
                                    else
                                        dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),dataobjectInfoList,job.getId(),invokingDataServiceStyle);
                                      
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.storeDataobjects failed! e="+e);

                                    if ( e instanceof TTransportException || dataService == null )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }
                        }

                        dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), dataobjectInfoList.size());
 
                        serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_COUNT, processSucceedCount);
                        long timeTaken = new Date().getTime()-startTime1.getTime();
                        serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_TIME_TAKEN, timeTaken); 
                  
                        processSucceedCount = 0;
                        dataobjectIdChecksumMap = new HashMap<>();
                    }
                    
                    if ( skippedCount > 0 )
                        dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(),skippedCount);
                          
                    JDBCUtil.close(resultSet);
                    JDBCUtil.close(statement);
                }
                
                if ( isPagenated == false ) // 不分页
                    break;
            }          

            return errorInfo.toString();
        }
        catch (Exception e)
        {
            String errorStr = "processFullSync() failed! countQuerySQL=["+countQuerySQL+"] querySQL="+querySQL+" e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            throw new Exception(errorStr);            
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(statement);
        }         
    }
    
    private String processChangeTable() throws Exception
    {
        int processSucceedCount = 0;
        PreparedStatement changeStatement = null;
        ResultSet changeResultSet = null;
        String dataobjectId;
        String changeQuerySQL = null;
        StringBuilder errorInfo = new StringBuilder();
        
        log.info("processChangeTable() ...");
       
        try
        {
            countQuerySQL = String.format("select count(*) from %s%s where table_name='%s' and change_time>'%s'",schema,changeTableName,databaseItem, getStartTime());
            getQueryCountOnly(countQuerySQL);
            
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), count);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 0);

            changeQuerySQL = String.format("select * from %s%s where table_name='%s' and change_time>'%s' order by change_time",schema,changeTableName,databaseItem, getStartTime());
            changeStatement = dbConn.prepareStatement(changeQuerySQL);
            changeStatement.setFetchSize(FETCH_SIZE);

            changeResultSet = JDBCUtil.executeQuery(changeStatement);

            log.info(String.format(" change querySql =%s ",changeQuerySQL));
            
            changeResultSet.last();
            int size = changeResultSet.getRow();
            dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), size);

            changeResultSet = JDBCUtil.executeQuery(changeStatement);
            //changeResultSet.beforeFirst();
            /*
             *   change table column:
            
                 id bigint,
                 change_type char(1), A-add,D-delete,U-update
                 table_name varchar(255);
                 primary_key_info varchar(1000), //id=1000 and b='23434' --- id=10000
                 change_time datetime, ...
             *
             *
            */
                    
            while(changeResultSet.next())
            {
                String changePrimaryKeyInfo = changeResultSet.getString("primary_key_info");
                String changeType = changeResultSet.getString("change_type");
                lastProcessedItem = Tool.convertDateToTimestampString(changeResultSet.getTimestamp("change_time")); 

                if ( changeType.toLowerCase().equals("add") || changeType.toLowerCase().equals("update") )
                {
                    if ( filter==null || filter.isEmpty() )
                        querySQL = String.format("select %s from %s%s where %s",columnNameListStr,schema,databaseItem,changePrimaryKeyInfo);
                    else
                        querySQL = String.format("select %s from %s%s where %s and %s",columnNameListStr,schema,databaseItem,changePrimaryKeyInfo,filter);
                    
                    statement = dbConn.prepareStatement(querySQL);

                    resultSet = JDBCUtil.executeQuery(statement);

                    log.info(String.format(" querySql =%s ",querySQL));
                    
                    resultSet.next(); // only one record
  
                    primaryKeyValue = changePrimaryKeyInfo; // change late ?????

                    log.info(" process single record... key="+primaryKeyValue);

                    dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(),job.getSourceApplicationId(),job.getDatasourceType(),job.getDatasourceId(),primaryKeyValue,job.getTargetRepositoryId());

                    try
                    {
                        processSingleRecord(dataobjectId,"",null);
                        processSucceedCount++;
                    }
                    catch(Exception e)
                    {
                        log.info(" processSingleRecord() failed! e="+e+" dataojbectId="+dataobjectId+" stacktrace="+ExceptionUtils.getStackTrace(e));                
                        errorInfo.append(String.format("primaryKeyValue=%s,dataobjectId=%s,errorInfoForEachRecord=(%s) stacktrace=(%s) \n\n",primaryKeyValue,dataobjectId,e.getMessage(),ExceptionUtils.getStackTrace(e)));

                        if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue() )
                            break;
                                  
                        dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);
                    }

                    if ( processSucceedCount >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                    {                               
                        dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);                
                        dataService.updateLastProcessedItem(job.getOrganizationId(), task.getId(), lastProcessedItem);

                        processSucceedCount = 0;
                        
                        while( commonService.isTooManyDataobjectInCache(job.getOrganizationId()) )
                        {
                            log.warn("too manay dataobject is in cache, stop collecting data! jobId="+job.getId());
                            Tool.SleepAWhile(10, 0); 
                            dataService.setDatasourceEndJobTime(job.getOrganizationId(), job.getId(), JobTimeType.LAST_UPDATED_TIME.getValue());
                        }

                        log.info("not too manay dataobject is in cache, continue collecting data! jobId="+job.getId());
                    }
                
                    resultSet.close();
                }
                else
                if( changeType.equals("delete") )
                {

                }                    
            }
                         
            if ( processSucceedCount > 0 )
            {
                dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                dataService.updateLastProcessedItem(job.getOrganizationId(), task.getId(), lastProcessedItem);
            }
            
            changeResultSet.close();
            
            return errorInfo.toString();
        }
        catch (Exception e)
        {
            String errorStr = "processChangeTable() failed! countQuerySQL=["+countQuerySQL+"] changeQuerySQL="+changeQuerySQL+"] querySQL="+querySQL+" e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            throw new Exception(errorStr);            
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(changeResultSet);
            JDBCUtil.close(statement);
        }        
    }
    
    private void deleteExistingRecord() throws Exception
    {
        String deleteSql;
        String startItem;
      
        String databaseItem1 = databaseItem;
        
        DatasourceConnection datasourceConnection1 = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());

        String schema1 = Util.getDatabaseSchema(datasourceConnection1);
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));

            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
        
        startItem = getNextProcessedItems();
       
        if ( filter==null || filter.trim().isEmpty() ) //String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value.substring(0, 19));
        {
            if ( comparingFieldType == MetadataDataType.STRING.getValue() )
                deleteSql = String.format("delete from %s%s where %s>'%s'",schema1,databaseItem1,comparingFieldName,startItem);
            else 
            if ( comparingFieldType == MetadataDataType.DATE.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    deleteSql = String.format("delete from %s%s where %s > %s",schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd"));
                else
                    deleteSql = String.format("delete from %s%s where %s > '%s'",schema1,databaseItem1,comparingFieldName,startItem);
            }
            else
            if (  comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    deleteSql = String.format("delete from %s%s where %s > %s",schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd hh24:mi:ss"));
                else
                    deleteSql = String.format("delete from %s%s where %s > '%s'",schema1,databaseItem1,comparingFieldName,startItem);
            }            
            else
                deleteSql = String.format("delete from %s%s where %s > %s",schema1,databaseItem1,comparingFieldName,startItem);
        }
        else
        {
             if ( comparingFieldType == MetadataDataType.STRING.getValue() )
                deleteSql = String.format("delete from %s%s where %s>'%s' and %s",schema1,databaseItem1,comparingFieldName,startItem,filter);
            else 
            if ( comparingFieldType == MetadataDataType.DATE.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    deleteSql = String.format("delete from %s%s where %s > %s and %s",schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd"),filter);
                else
                    deleteSql = String.format("delete from %s%s where %s > '%s' and %s",schema1,databaseItem1,comparingFieldName,startItem,filter);
            }
            else
            if (  comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    deleteSql = String.format("delete from %s%s where %s > %s and %s",schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd hh24:mi:ss"),filter);
                else
                    deleteSql = String.format("delete from %s%s where %s > '%s' and %s",schema1,databaseItem1,comparingFieldName,startItem,filter);
            }            
            else
                deleteSql = String.format("delete from %s%s where %s > %s and %s",schema1,databaseItem1,comparingFieldName,startItem,filter);
        }
         
        int retry = 0;
        Connection dbConn1 = null;
       
        while(true)
        {
            retry++;
            
            try
            {
                //DatasourceConnection datasourceConnection1 = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());

                dbConn1 = JDBCUtil.getJdbcConnection(datasourceConnection1);
                log.info(" dbConn ="+dbConn1+" delete sql="+deleteSql);
                
                dbConn1.setAutoCommit(false);
              
                PreparedStatement resultStatement = dbConn1.prepareStatement(deleteSql);
                resultStatement.executeUpdate();
                dbConn1.commit();
                
                log.info("finish delete! sql="+deleteSql);
                
                break;
            }
            catch(Exception e)
            {
                log.error(" delete existing record failed! e="+e);
                
                if ( retry > 3 )
                    throw e;
                
                Tool.SleepAWhile(1, 0);
            }       
            finally
            {
                JDBCUtil.close(dbConn1);
            }
        }
    }
    
    private String processOnlyNewRecord() throws Exception
    {
        log.info("processOnlyNewRecord() ...");
        
        int processSucceedCount;
        int unSentCount;
        String dataobjectId;
        Date startTime = new Date();
        long timeTaken;
        StringBuilder errorInfo = new StringBuilder();
        List<DataobjectInfo> dataobjectInfoList;
        long lastCallCount = 0;
        Date date;
        Date newDate;
        int currentQueryCount = -1;
        double countPerSecond = 0;
        
        double secondPlus = 10; // 10 minutes
        int NUMBER_INCREASE = 10000;
        int increaseFactor = 1;
            
        try
        {              
            deleteExistingRecord();
                
            countQuerySQL = generateNewRecordQuerySQLCount();
            getQueryCount(countQuerySQL);

            timeTaken = new Date().getTime()-startTime.getTime();
            log.info("execute count query, count="+count+" took ["+timeTaken+"] ms"+ " sql="+countQuerySQL);
       
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), count);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 0);
               
            if ( count == 0 )
            {
                log.info("count =0, no new data, return immediately!");
                return ""; // no new data, return
            }
            
            if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                try
                {
                    log.info(" minDate.getTime()="+minDate.getTime());
                    log.info(" maxDate.getTime()="+maxDate.getTime());
                    
                    double seconds = (double)((maxDate.getTime() - minDate.getTime())/1000);
                    
                    countPerSecond = (double)count/seconds;

                    if ( count < MAX_CALL_ROW_NUMBER )
                    {
                        secondPlus = seconds + 1;
                    }
                    else
                        secondPlus = (double)seconds/ ( (double)count/(double)MAX_CALL_ROW_NUMBER);
                    
                    if ( secondPlus < 1 )
                        secondPlus = 1;
 
                    log.info(" countPerMinute = "+countPerSecond+" TIME_INCREASE="+secondPlus+" range seconds="+seconds);
                }
                catch(Exception e)
                {
                    log.error("computing increase failed! e="+e);
                }
            }
            else
            {
                NUMBER_INCREASE = MAX_CALL_ROW_NUMBER;
            }
                 
            int k = 0;
            lastProcessedItem = null;
            boolean errorHappened = false;
            
            while(true)
            {
                Date startTime1 = new Date();
                processSucceedCount = 0;
                unSentCount = 0;
                
                k++;
              
                if ( lastProcessedItem == null )
                {
                    lastProcessedItem = getNextProcessedItems();

                    if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
                    {
                        date = Tool.convertStringToDate(lastProcessedItem, "yyyy-MM-dd HH:mm:ss");
                        
                        if ( date.before(minDate) )
                        {
                            newDate = Tool.dateAddDiff(minDate, -1, Calendar.SECOND);  // minus 1 second
                            lastProcessedItem = Tool.convertDateToTimestampString(newDate);
                        }
                    }
                    else
                    {
                        long lp = Long.parseLong(lastProcessedItem);
                        
                        if ( lp < Long.parseLong(minValue) )
                        {
                            lastProcessedItem = String.valueOf(Long.parseLong(minValue)-1);
                        }
                    }
                }
                                  
                if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
                {
                    if ( currentQueryCount >= 0 )
                    {
                        if ( currentQueryCount < secondPlus*countPerSecond/2 )
                            increaseFactor = increaseFactor*2;
                        else
                        if ( currentQueryCount > secondPlus*countPerSecond*2 )
                        {
                            increaseFactor = increaseFactor/2;
                            
                            if ( increaseFactor < 1 )
                                increaseFactor = 1;
                        }
                    }
                         
                    log.info("111111111 lastCallCount = "+lastCallCount+" secondPlus="+secondPlus+" increaseFactor="+increaseFactor);
                    lastProcessedItemPlus = getTimeLastprocessItemsPlus(lastProcessedItem,(int)secondPlus*increaseFactor); // take 10 minutes data
                }
                else // number
                {                
                    if ( currentQueryCount >= 0 )
                    {
                        if ( currentQueryCount < NUMBER_INCREASE/2 )
                            increaseFactor = increaseFactor*2;
                        else
                        if ( currentQueryCount > NUMBER_INCREASE*2 )
                        {
                            increaseFactor = increaseFactor/2;
                            
                            if ( increaseFactor < 1 )
                                increaseFactor = 1;
                        }
                    }
                    
                    log.info("22222222222 lastCallCount = "+lastCallCount+" increase="+NUMBER_INCREASE*increaseFactor);       
                    lastProcessedItemPlus = getLongLastprocessItemsPlus(lastProcessedItem,NUMBER_INCREASE*increaseFactor);
                }
                
                log.info(" lastProcessedItem="+lastProcessedItem+" lastProcessedItemPlus="+lastProcessedItemPlus+" maxValue="+maxValue+" minValue="+minValue);
              
                querySQL = generateNewRecordQuerySQL();
              
                startTime = new Date();
                        
                statement = dbConn.prepareStatement(querySQL);
                statement.setFetchSize(FETCH_SIZE);
 
                currentQueryCount = 0;
                
                resultSet = JDBCUtil.executeQuery(statement);

                timeTaken = new Date().getTime()-startTime.getTime();
                log.info(String.format("execute select query, took [%d] ms, sql=%s",timeTaken,querySQL));
        
                lastCallCount = 0;
                
                dataobjectInfoList = new ArrayList<>();
                
                if ( resultSet.next() == false )
                {
                    log.info("11111111 no result!");
                    
                    try {
                        resultSet.close();
                        statement.close();
                    }
                    catch(Exception e) {
                    }
                                        
                    if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
                    {
                         date = Tool.convertStringToDate(lastProcessedItem, "yyyy-MM-dd HH:mm:ss");
                         
                         if ( maxDate == null )
                         {
                             log.info(" maxDate == null, no more data");
                             break;
                         }
                         
                         if ( date.after(maxDate) )
                         {
                             log.info(" date > maxDate, exit");
                             break;
                         }
                    }
                    else
                    {
                        if ( maxValue == null )
                        {
                            log.info(" maxValue is null, no more data, exit!!!");
                            break;
                        }
                        
                        if ( Long.parseLong(lastProcessedItem) >= Long.parseLong(maxValue) )
                        {
                            log.info(" lastProcessedItem >= maxValue, exit!!!");
                            break;
                        }
                    }
                    
                    int retry=0;
                    
                    while(true)
                    {
                        retry++;
                        
                        log.info(" updateLastProcessedItem retry="+retry);
                        
                        try
                        {
                           dataService.updateLastProcessedItem(job.getOrganizationId(), task.getId(), lastProcessedItemPlus);          
                           break;
                        }
                        catch(Exception e)
                        {
                            log.info("updateLastProcessedItem failed! taskId="+task.getId()+" lastProcessedItemPlus="+lastProcessedItemPlus);
                            
                            if ( e instanceof TTransportException )
                            {
                                try{
                                    dataService = dsConn.reConnect();
                                }
                                catch(Exception ee) {
                                }
                            }
                            
                            Tool.SleepAWhile(1, 0);

                            if ( retry > 10 )
                                throw e;
                        }
                    }
                }
                else
                {
                    do
                    {
                        currentQueryCount++;
                        lastCallCount ++;
                        
                        primaryKeyValue = getPrimaryKeyValue(primaryKeyStr,resultSet,columnNameListStr);

                        //log.info("processOnlyNewRecord(): process single record... key="+primaryKeyValue);

                        dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(),job.getSourceApplicationId(),job.getDatasourceType(),job.getDatasourceId(),primaryKeyValue,job.getTargetRepositoryId());

                        try
                        {
                            processSingleRecord(dataobjectId,"",dataobjectInfoList);
                            
                            processSucceedCount++;
                            unSentCount++;

                            if ( unSentCount >= CommonKeys.DATASOURCE_END_TASK_DATAOBJECT_BATCH_NUM && task.getTargetRepositoryType() != TargetRepositoryType.SQL_DB.getValue() )
                            {
                                startTime = new Date();
                                
                                int retry = 0;
                    
                                while(true)
                                {
                                    retry ++;

                                    try
                                    {
                                        dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),dataobjectInfoList,job.getId(),invokingDataServiceStyle);                         
                                        break;
                                    }
                                    catch(Exception e)
                                    {   
                                        log.error("process data failed! e="+e);
                                    
                                        if ( e instanceof TTransportException || dataService == null )
                                        {
                                            try{
                                                dataService = dsConn.reConnect();
                                            }
                                            catch(Exception ee) {
                                            }
                                        }

                                        Tool.SleepAWhile(1, 0);
                                        if ( retry > 3 )
                                        {
                                            log.error("process data failed! retry > 3 throw error!");
                                            throw e;
                                        }
                                    }
                                }
                           
                                timeTaken = new Date().getTime()-startTime.getTime();
                                log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );
                                
                                dataobjectInfoList = new ArrayList<>();
                                unSentCount = 0;
                            }                         
        
                            if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
                                lastProcessedItemInDB = Tool.convertDateToTimestampString(resultSet.getTimestamp(comparingFieldName));
                            else
                                lastProcessedItemInDB = resultSet.getString(comparingFieldName);
                        }
                        catch(Exception e)
                        {
                            log.info(" processSingleRecord() failed! e="+e+"  dataojbectId="+dataobjectId+" stacktrace="+ExceptionUtils.getStackTrace(e));

                            errorInfo.append(String.format("processSingleRecord() failed! comparingFieldName=%s,lastProcessedItem=%s,errorInfoForEachRecord=(%s) stacktrace=(%s)\n\n", comparingFieldName,lastProcessedItem,e.getMessage(),ExceptionUtils.getStackTrace(e)));
                            errorHappened = true;
                            break;
                        }

                        if ( processSucceedCount >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM && task.getTargetRepositoryType() != TargetRepositoryType.SQL_DB.getValue() )
                        {
                            startTime = new Date();
                            
                            int retry = 0;
                    
                            while(true)
                            {
                                retry ++;

                                try
                                {
                                    dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),dataobjectInfoList,job.getId(),invokingDataServiceStyle);                               
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.storeDataobjects failed! e="+e);

                                    if ( e instanceof TTransportException )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }

                            timeTaken = new Date().getTime()-startTime.getTime();
                            log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );

                            dataobjectInfoList = new ArrayList<>();
                            unSentCount = 0;
                                
                            retry = 0;
                            while(true)
                            {
                                retry ++;

                                try
                                {
                                    dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.increaseDatasourceEndJobItemNumber failed! e="+e);

                                    if ( e instanceof TTransportException )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }
                            
                            retry = 0;
                            while(true)
                            {
                                retry ++;

                                try
                                {              
                                    dataService.updateLastProcessedItem(job.getOrganizationId(), task.getId(), lastProcessedItemInDB);         
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.updateLastProcessedItem failed! e="+e);

                                    if ( e instanceof TTransportException )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }
 
                            processSucceedCount = 0;

                          /*  while( commonService.isTooManyDataobjectInCache(job.getOrganizationId()) )
                            {
                                log.warn("too manay dataobject is in cache, stop collecting data! jobId="+job.getId());
                                Tool.SleepAWhile(10, 0); 
                                dataService.setDatasourceEndJobTime(job.getOrganizationId(), job.getId(), JobTimeType.LAST_UPDATED_TIME.getValue());
                            }

                            log.info("not too manay dataobject is in cache, continue collecting data! jobId="+job.getId());*/
                        }
                    }
                    while(resultSet.next());
                    
                    try {                  
                        resultSet.close();
                        statement.close();
                    }
                    catch(Exception e) {
                    }
                    
                    log.info(" unsendCount ="+unSentCount);
                    
                    if ( unSentCount > 0 )
                    {
                        startTime = new Date();

                        int retry = 0;
                    
                        while(true)
                        {
                            retry ++;

                            try
                            {
                                if ( task.getTargetRepositoryType() == TargetRepositoryType.SQL_DB.getValue() )
                                {
                                    if ( sqldbWriter == null )
                                    {
                                        sqldbWriter = new SqldbWriter();

                                        dataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(targetDataobjectTypeId).array());

                                        List<Map<String, Object>> metadataDefinitions = (List<Map<String, Object>>)Tool.deserializeObject(dataService.getDataobjectTypeMetadataDefinition(targetDataobjectTypeId, false, true).array());

                                        DatasourceConnection datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());

                                        sqldbWriter.setConfig(task.getOrganizationId(), job.getTargetRepositoryId(),dataobjectType.getName(),dataobjectType,metadataDefinitions,datasourceConnection);
                                    }
                                    
                                    sqldbWriter.processData(dataobjectInfoList);
                                }
                                else
                                    dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),dataobjectInfoList,job.getId(),invokingDataServiceStyle);
                               
                                 break;
                            }
                            catch(Exception e)
                            {   
                                log.error("dataService.storeDataobjects failed! e="+e);

                                if ( e instanceof TTransportException )
                                {
                                    try{
                                        dataService = dsConn.reConnect();
                                    }
                                    catch(Exception ee) {
                                    }
                                }

                                Tool.SleepAWhile(1, 0);
                                if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                {
                                    log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                    throw e;
                                }
                            }
                        }

                        timeTaken = new Date().getTime()-startTime.getTime();
                        log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );
                    }
                    
                    if ( processSucceedCount > 0 )
                    {                                   
                        dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                    
                        String newLastProcessedItem = lastProcessedItemInDB;
                    
                        if ( lastProcessedItemDelay > 0 && comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
                        {
                            try 
                            {           
                               Date date1 = Tool.convertStringToDate(lastProcessedItemInDB, "yyyy-MM-dd HH:mm:ss");
                               date1 = Tool.dateAddDiff(date1, -1*lastProcessedItemDelay, Calendar.MINUTE);                       
                               newLastProcessedItem = Tool.convertDateToString(date1, "yyyy-MM-dd HH:mm:ss");

                               log.info(" lastProcessedItemInDB ="+lastProcessedItemInDB+" newLastProcessedItem="+newLastProcessedItem);
                            }
                            catch(Exception e)
                            {
                                log.error(" convert faield! e="+e);
                            }
                        }
                        
                        dataService.updateLastProcessedItem(job.getOrganizationId(), task.getId(), newLastProcessedItem);
                    }

                    if ( errorHappened )
                        break;
                }
                
                lastProcessedItem = lastProcessedItemPlus;     
                
                serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_COUNT, lastCallCount);
                timeTaken = new Date().getTime()-startTime1.getTime();
                serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_TIME_TAKEN, timeTaken);  
            }
            
            return errorInfo.toString();
        }
        catch (Exception e)
        {
            String errorStr = " processOnlyNewRecord() failed! countQuerySQL=["+countQuerySQL+"] querySQL="+querySQL+" e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            throw new Exception(errorStr);
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(statement);
          /*  
            if ( sqldbWriter != null )
            {
                sqldbWriter.quitThread();
            }*/
        }
    }
    
    private void deleteExistingRecordForString() throws Exception
    {
        String deleteSql;
        String startItem;
 
        String databaseItem1 = databaseItem;
        
        DatasourceConnection datasourceConnection1 = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());

        String schema1 = Util.getDatabaseSchema(datasourceConnection1);
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));

            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
        
        startItem = getNextProcessedItems();
       
        if ( filter==null || filter.trim().isEmpty() ) //String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value.substring(0, 19));
            deleteSql = String.format("delete from %s%s where %s>'%s'",schema1,databaseItem1,comparingFieldName,startItem);
        else
            deleteSql = String.format("delete from %s%s where %s>'%s' and ( %s )",schema1,databaseItem1,comparingFieldName,startItem,filter);

        int retry = 0;
        Connection dbConn1 = null;
       
        while(true)
        {
            retry++;
            
            try
            {
                //DatasourceConnection datasourceConnection1 = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());

                dbConn1 = JDBCUtil.getJdbcConnection(datasourceConnection1);
                log.info(" dbConn ="+dbConn1+" delete sql="+deleteSql);
                
                dbConn1.setAutoCommit(false);
              
                PreparedStatement resultStatement = dbConn1.prepareStatement(deleteSql);
                resultStatement.executeUpdate();
                dbConn1.commit();
                
                log.info("finish delete! sql="+deleteSql);
                
                break;
            }
            catch(Exception e)
            {
                log.error(" delete existing record failed! e="+e);
                
                if ( retry > 3 )
                    throw e;
                
                Tool.SleepAWhile(1, 0);
            }       
            finally
            {
                JDBCUtil.close(dbConn1);
            }
        }
    }
  
    private String processOnlyNewRecordForString() throws Exception
    {
        log.info("processOnlyNewRecordForString() ... comparingFieldName="+comparingFieldName);
        
        int processSucceedCount;
        int unSentCount;
        String dataobjectId;
        Date startTime = new Date();
        long timeTaken;
        StringBuilder errorInfo = new StringBuilder();
        List<DataobjectInfo> dataobjectInfoList;
        long lastCallCount = 0;
 
        try
        {
            deleteExistingRecordForString();
                        
            countQuerySQL = generateNewRecordQuerySQLCountForString();
            getQueryCount(countQuerySQL);

            timeTaken = new Date().getTime()-startTime.getTime();
            log.info("232323 execute count query, count="+count+" took ["+timeTaken+"] ms"+ " sql="+countQuerySQL);

            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), count);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 0);

            if ( count == 0 )
            {
                log.info("count =0, no new data, return immediately!");
                return ""; // no new data, return
            }
                 
            int k = 0;
            lastProcessedItem = null;
            boolean errorHappened = false;
            List<String> lastRunKeys = new ArrayList<>();
            List<String> currentRunKeys = new ArrayList<>();
            List<String> historyRunKeys = new ArrayList<>();
            
            boolean notEnd = false;
                   
            int pageSize = 16000;
            boolean isFirstTime = true;
            boolean isFirstTimeFinish = true;
          
            while(true)
            {
                Date startTime1 = new Date();
                processSucceedCount = 0;
                unSentCount = 0;
                
                k++;
              
                if ( lastProcessedItem == null )
                    lastProcessedItem = getNextProcessedItems(); 
                                                 
                log.info(" lastProcessedItem="+lastProcessedItem+"  maxValue="+maxValue+" minValue="+minValue);
                
                querySQL = generateNewRecordQuerySQLForString(databaseType,1,pageSize,isFirstTime);
              
                isFirstTime = false;
                
                startTime = new Date();
                        
                statement = dbConn.prepareStatement(querySQL);
                statement.setFetchSize(FETCH_SIZE);
  
                resultSet = JDBCUtil.executeQuery(statement);

                timeTaken = new Date().getTime()-startTime.getTime();
                log.info(String.format("execute select query, took [%d] ms, sql=%s",timeTaken,querySQL));
        
                lastCallCount = 0;
                
                dataobjectInfoList = new ArrayList<>();
                
                if ( resultSet.next() == false )
                {
                    log.info("11111111 no result!");
                    
                    try {
                        resultSet.close();
                        statement.close();
                    }
                    catch(Exception e) {
                    }
                             
                    break;
                }
                else
                {
                    currentRunKeys = new ArrayList<>();
                    
                    do
                    {
                        primaryKeyValue = getPrimaryKeyValue(primaryKeyStr,resultSet,columnNameListStr);

                        log.info("processOnlyNewRecord(): process single record... key="+primaryKeyValue);
                         
                        if ( historyRunKeys.contains(primaryKeyValue) )
                        {
                            log.info(" already processed! skip");
                            continue;
                        }
                        else                       
                            historyRunKeys.add(primaryKeyValue);                                               
                         
                        currentRunKeys.add(primaryKeyValue);
                                                
                        dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(),job.getSourceApplicationId(),job.getDatasourceType(),job.getDatasourceId(),primaryKeyValue,job.getTargetRepositoryId());

                        try
                        {
                            processSingleRecord(dataobjectId,"",dataobjectInfoList);
                            
                            processSucceedCount++;
                            unSentCount++;

                            if ( unSentCount >= CommonKeys.DATASOURCE_END_TASK_DATAOBJECT_BATCH_NUM && task.getTargetRepositoryType() != TargetRepositoryType.SQL_DB.getValue() )
                            {
                                startTime = new Date();
                                
                                int retry = 0;
                    
                                while(true)
                                {
                                    retry ++;

                                    try
                                    {
                                        dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),dataobjectInfoList,job.getId(),invokingDataServiceStyle);                         
                                        break;
                                    }
                                    catch(Exception e)
                                    {   
                                        log.error("process data failed! e="+e);
                                    
                                        if ( e instanceof TTransportException || dataService == null )
                                        {
                                            try{
                                                dataService = dsConn.reConnect();
                                            }
                                            catch(Exception ee) {
                                            }
                                        }

                                        Tool.SleepAWhile(1, 0);
                                        if ( retry > 3 )
                                        {
                                            log.error("process data failed! retry > 3 throw error!");
                                            throw e;
                                        }
                                    }
                                }
                           
                                timeTaken = new Date().getTime()-startTime.getTime();
                                log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );
                                
                                dataobjectInfoList = new ArrayList<>();
                                unSentCount = 0;
                            }                         
        
                            lastProcessedItemInDB = resultSet.getString(comparingFieldName);
                        }
                        catch(Exception e)
                        {
                            log.info(" processSingleRecord() failed! e="+e+"  dataojbectId="+dataobjectId+" stacktrace="+ExceptionUtils.getStackTrace(e));

                            errorInfo.append(String.format("processSingleRecord() failed! comparingFieldName=%s,lastProcessedItem=%s,errorInfoForEachRecord=(%s) stacktrace=(%s)\n\n", comparingFieldName,lastProcessedItem,e.getMessage(),ExceptionUtils.getStackTrace(e)));
                            errorHappened = true;
                            break;
                        }

                        if ( processSucceedCount >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM && task.getTargetRepositoryType() != TargetRepositoryType.SQL_DB.getValue() )
                        {
                            startTime = new Date();
                            
                            int retry = 0;
                    
                            while(true)
                            {
                                retry ++;

                                try
                                {
                                    dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),dataobjectInfoList,job.getId(),invokingDataServiceStyle);                               
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.storeDataobjects failed! e="+e);

                                    if ( e instanceof TTransportException )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }

                            timeTaken = new Date().getTime()-startTime.getTime();
                            log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );

                            dataobjectInfoList = new ArrayList<>();
                            unSentCount = 0;
                                
                            retry = 0;
                            while(true)
                            {
                                retry ++;

                                try
                                {
                                    dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.increaseDatasourceEndJobItemNumber failed! e="+e);

                                    if ( e instanceof TTransportException )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }
                            
                            retry = 0;
                            while(true)
                            {
                                retry ++;

                                try
                                {              
                                    dataService.updateLastProcessedItem(job.getOrganizationId(), task.getId(), lastProcessedItemInDB);         
                                    break;
                                }
                                catch(Exception e)
                                {   
                                    log.error("dataService.updateLastProcessedItem failed! e="+e);

                                    if ( e instanceof TTransportException )
                                    {
                                        try{
                                            dataService = dsConn.reConnect();
                                        }
                                        catch(Exception ee) {
                                        }
                                    }

                                    Tool.SleepAWhile(1, 0);
                                    if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                    {
                                        log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                        throw e;
                                    }
                                }
                            }
 
                            processSucceedCount = 0;
                        }
                    }
                    while(resultSet.next());
                    
                    try {                  
                        resultSet.close();
                        statement.close();
                    }
                    catch(Exception e) {
                    }
                    
                    log.info(" unsendCount ="+unSentCount);
                    
                    if ( unSentCount > 0 )
                    {
                        startTime = new Date();

                        int retry = 0;
                    
                        while(true)
                        {
                            retry ++;

                            try
                            {
                                if ( task.getTargetRepositoryType() == TargetRepositoryType.SQL_DB.getValue() )
                                {
                                    if ( sqldbWriter == null )
                                    {
                                        sqldbWriter = new SqldbWriter();

                                        dataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(targetDataobjectTypeId).array());

                                        List<Map<String, Object>> metadataDefinitions = (List<Map<String, Object>>)Tool.deserializeObject(dataService.getDataobjectTypeMetadataDefinition(targetDataobjectTypeId, false, true).array());

                                        DatasourceConnection datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),job.getTargetRepositoryId()).array());

                                        sqldbWriter.setConfig(task.getOrganizationId(), job.getTargetRepositoryId(),dataobjectType.getName(),dataobjectType,metadataDefinitions,datasourceConnection);
                                    }
                                    
                                    sqldbWriter.processData(dataobjectInfoList);
                                }
                                else
                                    dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),dataobjectInfoList,job.getId(),invokingDataServiceStyle);
                               
                                 break;
                            }
                            catch(Exception e)
                            {   
                                log.error("dataService.storeDataobjects failed! e="+e);

                                if ( e instanceof TTransportException )
                                {
                                    try{
                                        dataService = dsConn.reConnect();
                                    }
                                    catch(Exception ee) {
                                    }
                                }

                                Tool.SleepAWhile(1, 0);
                                if ( retry > CommonKeys.DATASERVICE_RETRY_TIMES )
                                {
                                    log.error("dataService.storeDataobjects failed! retry > "+CommonKeys.DATASERVICE_RETRY_TIMES+" throw error!");
                                    throw e;
                                }
                            }
                        }

                        timeTaken = new Date().getTime()-startTime.getTime();
                        log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );
                    }
                              
                    if ( errorHappened )
                        break;
                }
                
                notEnd = false;
                
                if ( currentRunKeys.size() != lastRunKeys.size() )
                    notEnd = true;
                else
                {
                    for(int i=0;i<currentRunKeys.size();i++)
                    {
                        if ( !lastRunKeys.get(i).equals(currentRunKeys.get(i)) )
                        {
                            notEnd = true;
                            break;
                        }
                    }
                }
                
                if ( notEnd == false )
                    break;
                           
                if ( processSucceedCount > 0 )
                {                                   
                    dataService.increaseDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                    
                    String newLastProcessedItem = lastProcessedItemInDB;
                    
                    if ( lastProcessedItemDelay > 0 )
                    {
                        try 
                        {                      
                           Date date = Tool.convertStringToDate(lastProcessedItemInDB, "yyyyMMddHHmmss");
                           date = Tool.dateAddDiff(date, -1*lastProcessedItemDelay, Calendar.MINUTE);                       
                           newLastProcessedItem = Tool.convertDateToString(date, "yyyyMMddHHmmss");

                           log.info(" lastProcessedItemInDB ="+lastProcessedItemInDB+" newLastProcessedItem="+newLastProcessedItem);
                        }
                        catch(Exception e)
                        {
                            log.error(" convert faield! e="+e);
                        }
                    }
              
                    dataService.updateLastProcessedItem(job.getOrganizationId(), task.getId(), newLastProcessedItem);
                }
                
                lastProcessedItem = lastProcessedItemInDB;
                                
                lastRunKeys = currentRunKeys;
                
                serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_COUNT, lastCallCount);
                timeTaken = new Date().getTime()-startTime1.getTime();
                serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_TIME_TAKEN, timeTaken);  
            }
            
            return errorInfo.toString();
        }
        catch (Exception e)
        {
            String errorStr = " processOnlyNewRecordForString() failed! countQuerySQL=["+countQuerySQL+"] querySQL="+querySQL+" e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            throw new Exception(errorStr);
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(statement);
            
           /* if ( sqldbWriter != null )
            {
                sqldbWriter.quitThread();
            }*/
        }
    }

    private String getTimeLastprocessItemsPlus(String lastProcessItems,int secondsToPlus)
    {
        Date date = Tool.convertStringToDate(lastProcessItems, "yyyy-MM-dd HH:mm:ss");
        date = Tool.dateAddDiff(date, secondsToPlus, Calendar.SECOND);
        return Tool.convertDateToTimestampString(date);
    }
    
    private String getLongLastprocessItemsPlus(String lastProcessItems,int plus)
    {
        long k = Long.parseLong(lastProcessItems)+plus;
        return String.valueOf(k);
    }
          
    private void processSingleRecord(String dataobjectId,String checksum,List<DataobjectInfo> list) throws Exception, SQLException, TException 
    {
        String fileLink;
        String accessFileLink = "";
        String finalAccessFileLink = "";
        totalSize = 0;
        Date startTime = new Date();
               
        obj = new DataobjectInfo();
               
        obj.setDataobjectId(dataobjectId);
        obj.setSourceApplicationId(job.getSourceApplicationId());
        obj.setDatasourceType(job.getDatasourceType());
        obj.setDatasourceId(job.getDatasourceId());
        obj.setDataobjectType(getDataobjectTypeId(obj));
        obj.setSourcePath(String.format("%d/%s/%s",datasourceConnection.getId(),databaseItem,primaryKeyValue));
        obj.setChecksum(checksum);
        
        obj.setMetadataList(new ArrayList<FrontendMetadata>());
        metadataList = obj.getMetadataList();
        
        contentBinaries = new ArrayList<>();
        contentIds = new ArrayList<>();
        contentNames = new ArrayList<>();
        contentLocations = new ArrayList<>();
        needToExtractContentTexts = new ArrayList<>();
        
        needToSaveContentInSystems = new ArrayList<>();
        obj.setSaveContentInSystems(needToSaveContentInSystems);
        
        dataobjectNameStr = "";
        
        for( Map<String,Object> map : databaseItemDefinitions )
        {
            String fieldName = (String)map.get("name");
            boolean isFileLink = (Boolean)map.get("isFileLink");
            boolean useAsDataobjectName = (Boolean)map.get("useAsDataobjectName");
            int dataType = Integer.parseInt((String)map.get("dataType"));
            String datetimeFormat = (String)map.get("datetimeFormat");            
            String targetDataobjectMetadataName = (String)map.get("targetDataobjectMetadataName");
            //int targetDataobjectMetadataType = Integer.parseInt((String)map.get("targetDataobjectMetadataType"));
            
            //if ( targetDataobjectMetadataType != dataType )
            //    dataType = targetDataobjectMetadataType;
            
            needToSaveContentInSystem = (Boolean)map.get("backupDocument");
            
            if ( dataType == MetadataDataType.BINARY.getValue() || isFileLink )
            {
                InputStream in;
                
                if ( isFileLink )
                {
                    fileLink = resultSet.getString(fieldName);
                    
                    if ( fileLink != null && !fileLink.isEmpty() )
                    {
                        if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                        {
                            accessFileLink = getAbsoluteFileLink(Tool.convertToWindowsPathFormat(fileLink));
                            contentBinary = FileUtil.readFileToByteBuffer(new File(accessFileLink));
                        }
                        else
                        {
                            accessFileLink = getNonWindowsAccessFileLink(Tool.convertToNonWindowsPathFormat(fileLink));
                            finalAccessFileLink = accessFileLink.substring(accessFileLink.lastIndexOf(':')+1);   // remove datasource conn
                            
                            contentBinary = FileUtil.readFileToByteBuffer(new File(finalAccessFileLink));  
                        }
                    }
                    else
                        contentBinary = null;
                    
                    log.info("search fileLink="+fileLink+" accessFileLink="+accessFileLink+ " finalAccessFileLink="+ finalAccessFileLink);
                    
                    if ( contentBinary == null)
                        log.error("can not find file link!");
                    else
                        log.debug("found file! size="+contentBinary.capacity());
                }
                else // not a file link, is CLOB or BLOB in table
                {                    
                    String value = resultSet.getString(fieldName);
                    
                    if ( value==null || value.isEmpty() )
                        contentBinary = null;
                    else
                        contentBinary = ByteBuffer.wrap(value.getBytes("UTF-8"));
                }
                
                if ( contentBinary!=null && contentBinary.capacity()>0)
                {
                    totalSize += contentBinary.capacity();
                                    
                    contentBinaries.add(contentBinary);
                    contentId = DataIdentifier.generateContentId(contentBinary);
                    contentIds.add(contentId);
                    contentName = fieldName+"_content";
                    contentNames.add(contentName);
                    needToSaveContentInSystems.add(needToSaveContentInSystem);

                    boolean isContentExisting = dataService.isContentExisting(job.getOrganizationId(),contentId);                    
                    
                    if ( needToSaveContentInSystem == false)
                    {
                         if ( isFileLink )
                             contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_FILE_SERVER,accessFileLink));
                         else
                             contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_DATABASE,obj.getSourcePath()));
                    }
                    else
                         contentLocations.add(String.format("%s:%s",CommonKeys.CONTENT_LOCATION_BLOB_STORE,contentId));
                    
                    if ( !isContentExisting && needToSaveContentInSystem )
                        ContentStoreUtil.storeContent(job.getOrganizationId(),dataService,computingNode.getNodeOsType(),contentId,contentBinary,objectStorageServiceInstance,objectStoreServiceProviderProperties);
                    
                    if ( !isContentExisting && !needToSaveContentInSystem )
                        needToExtractContentTexts.add(true);
                    else
                        needToExtractContentTexts.add(false);
                }
            }
                    
            if ( dataType != MetadataDataType.BINARY.getValue() )
            {
                try
                {
                    fieldValue = resultSet.getString(fieldName);
                }
                catch(Exception e)
                {
                    log.error("111 resultSet.getString() failed! fieldName="+fieldName+", e="+e);
                    fieldValue = "";
                }
        
                if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue() ) // normal date and time
                { 
                    if ( dataType == MetadataDataType.DATE.getValue() )   
                    {
                        Date date = null;
                        
                        try {
                           date = resultSet.getDate(fieldName); 
                        }
                        catch(Exception e)
                        {
                            log.error("222 resultSet.getDate() failed! fieldName="+fieldName+", e="+e);
                        }
                        
                        if ( date == null )
                            fieldValue= "";
                        else
                            fieldValue = String.valueOf(date.getTime());
                    }      
                    else
                    if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                    {
                        Timestamp timestamp = null;

                        try {
                           timestamp = resultSet.getTimestamp(fieldName); 
                        }
                        catch(Exception e)
                        {
                            log.error("333 resultSet.getTimestamp() failed! fieldName="+fieldName+", e="+e);
                        }
                                                       
                        if ( timestamp == null )
                            fieldValue= "";
                        else
                            fieldValue = String.valueOf(timestamp.getTime());
                    }
                }
                else
                if ( dataType == MetadataDataType.STRING.getValue() && datetimeFormat!=null && !datetimeFormat.trim().isEmpty() ) // date and time from string
                {
                    try
                    {
                        Date date = Tool.convertDateStringToDateAccordingToPattern(fieldValue, datetimeFormat);

                        if ( date == null )
                            newFieldValue = "";
                        else
                            newFieldValue = String.valueOf(date.getTime());
                    }
                    catch(Exception e)
                    {
                        log.error(" convert datetime error! e="+e);
                        newFieldValue = "";
                    }                 
                    
                    if ( !newFieldValue.isEmpty() ) // add string to date,time field
                    {
                        int dataType1 = Util.getDateTimeDataType(datetimeFormat);
           
                        metadata = new FrontendMetadata(targetDataobjectMetadataName+"_"+MetadataDataType.findByValue(dataType1).name(),true);
                        metadata.setSingleValue(newFieldValue);
                        metadataList.add(metadata);
                    }
                }
                
                if ( fieldValue != null )
                {
                    if ( useAsDataobjectName )
                        dataobjectNameStr += fieldValue.trim()+"-";  
                          
                    metadata = new FrontendMetadata(targetDataobjectMetadataName,true);
                    metadata.setSingleValue(fieldValue.trim());
                    metadataList.add(metadata);
                }
            }
        }
        
        if ( dataobjectNameStr.length()== 0)
            obj.setName(primaryKeyValue);
        else
            obj.setName(dataobjectNameStr.substring(0,dataobjectNameStr.length()-1));
        
        obj.setContentIds(contentIds);
        obj.setContentNames(contentNames);
        obj.setSize(totalSize);
        obj.setIsPartialUpdate(false);
        obj.setNeedToSaveImmediately(false);
        
        collectDataobjectMetadata(obj,"");
        
        collectDataobjectContentMetadata(dataService,datasource,job,obj,contentIds,contentBinaries,contentLocations,needToExtractContentTexts,"");
        
        beforeStoringToServer(obj);
        
        if ( dataProcessingExtension != null )
            dataProcessingExtension.dataProcessing(obj, contentIds, contentBinaries);
        
        if ( list != null )
        {
            list.add(obj);
            return;
        }
        
        list = new ArrayList<>();
        list.add(obj);
        
        dataService.storeDataobjects(job.getOrganizationId(),task.getTargetRepositoryType(),job.getTargetRepositoryId(),list,job.getId(),invokingDataServiceStyle);
        
        long timeTaken = new Date().getTime()-startTime.getTime();
        log.info(String.format("processSingleRecord dataobjectId=[%s], took [%d] ms",dataobjectId,timeTaken) );
    }
    
    private String getPrimaryKeyValue(String primaryKeyStr, ResultSet resultSet,String columnNameListStr) throws SQLException 
    {
        String str = "";
        String [] columns;
        String value;
        
        log.debug(" primaryKeysStr ="+primaryKeyStr+" columnnameListStr="+columnNameListStr);
        
        if ( primaryKeyStr == null || primaryKeyStr.trim().isEmpty() )  // use all column as primary key
             columns = columnNameListStr.split("\\,");
        else
            columns = primaryKeyStr.split("\\.");

        for(String column:columns)
        {
            String newColumn = column;
            
            if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
                newColumn = Tool.removeAroundQuotation(column);
            
            log.debug("888888 get column value, column name="+newColumn);
            
            value = resultSet.getString(newColumn);
            if ( value == null || value.equals("null") )
                value = "";
            //else
            //    value = value.trim();
            
            str += value +".";
        }

        str = str.substring(0, str.length()-1);
        
        return str;
    }
    
    private String getRecordChecksumStr(String columnNameListStr, ResultSet resultSet) throws SQLException 
    {
        String columnStr;
        StringBuilder builder = new StringBuilder();
        String [] columns = columnNameListStr.split("\\,");
        
        for(String column:columns)
        {
            columnStr = column;
            
            if ( column.startsWith("\"") )
                columnStr = Tool.removeAroundQuotation(column);
                
            builder.append(resultSet.getString(columnStr)).append(";");
        }
         
         return builder.toString();
    }
         
    private void getDatasourceConfig(String datasourceConfig) throws Exception
    {
        String databaseSelectionStr = null;
            
        try 
        {
            SAXReader saxReader = new SAXReader();
            ByteArrayInputStream in = new ByteArrayInputStream(datasourceConfig.getBytes("utf-8"));
            Document datasourcePropertiesXml= saxReader.read(in);

            List<Element> nodes = datasourcePropertiesXml.selectNodes("//datasourceProperties/property");

            for( Element node: nodes)
            {
                if ( node.element("name").getTextTrim().equals("table_view_select") )
                {
                    databaseSelectionStr = (String)node.element("value").getData();
                    break;
                }
            }
            
            if ( databaseSelectionStr == null)
                throw new Exception("22");
            
            String databaseSelectionXml = String.format("<databaseSelection>%s</databaseSelection>", databaseSelectionStr);
            
            saxReader = new SAXReader();
            in = new ByteArrayInputStream(databaseSelectionXml.getBytes("utf-8"));
            Document doc = saxReader.read(in);            
            
            Element element = (Element)doc.selectSingleNode("//databaseSelection");           
            
            databaseItemType = Integer.parseInt(element.element("databaseItemType").getTextTrim());
            databaseItem = element.element("databaseItemName").getTextTrim();
            targetDataobjectTypeId =  Integer.parseInt(element.element("targetDataobjectType").getTextTrim());
            databaseArchiveMethodType =  Integer.parseInt(element.element("databaseArchiveMethodType").getTextTrim());
           
            if ( databaseArchiveMethodType == DatabaseArchiveMethodType.SYNC_BY_UPDATED_TABLE_FIELD.getValue() )
            {
                comparingFieldName = element.element("comparingFieldName").getTextTrim();
                comparingFieldDataType = element.element("comparingFieldDataType").getTextTrim();

                comparingFieldValue = element.element("comparingFieldValue").getTextTrim();      
                primaryKeyStr = element.element("primaryKeyStr").getTextTrim();
            }
        //    else
        //    if ( databaseArchiveMethodType == DatabaseArchiveMethodType.SYNC_BY_CHANGE_TABLE.getValue() )
        //    {
        //        changeTableName = element.element("changeTableName").getTextTrim();
        //        archiveStartTime = Tool.convertStringToDate(element.element("archiveStartTime").getTextTrim());
        //    }
            else
            if ( databaseArchiveMethodType == DatabaseArchiveMethodType.SYNC_BY_DETECTING_CONTENT_CHANGE.getValue() )
            {
                primaryKeyStr = element.element("primaryKeyStr").getTextTrim();
            }            
           
            filter =element.element("filter").getTextTrim();
            dataProcessingExtensionId = Integer.parseInt(element.element("dataProcessingExtensionId").getTextTrim());
            
            if ( element.element("isChangeableData") == null )
                isChangeableData = false;
            else
                isChangeableData = element.element("isChangeableData").getTextTrim().equals("true")?true:false;
            
            if ( element.element("needToProcessComparingFieldNullValueData") == null )
                needToProcessComparingFieldNullValueData = false;
            else
                needToProcessComparingFieldNullValueData = element.element("needToProcessComparingFieldNullValueData").getTextTrim().equals("true")?true:false;
            
            invokingDataServiceStyle = isChangeableData?true:false;  // 是变化数据，用同步方式调用dataservice
            
            log.info("111111111 isChangeableData = "+isChangeableData);
            
            nodes = doc.selectNodes("//databaseSelection/fields/field");
            
            databaseItemDefinitions = new ArrayList<>();
                    
            for( Element node : nodes)
            {
                definition = new HashMap<>();
                definition.put("name", node.element("name").getTextTrim());
                definition.put("dataType",node.element("dataType").getTextTrim());
                definition.put("isFileLink",node.element("isFileLink").getTextTrim().equals("true"));
                definition.put("backupDocument", node.element("backupDocument").getTextTrim().equals("true"));
                definition.put("useAsDataobjectName",node.element("useAsDataobjectName").getTextTrim().equals("true"));
                definition.put("targetDataobjectMetadataName",node.element("targetDataobjectMetadataName").getTextTrim());
                definition.put("targetDataobjectMetadataType",node.element("targetDataobjectMetadataType").getTextTrim());
                definition.put("datetimeFormat", node.element("datetimeFormat").getTextTrim());
                
                databaseItemDefinitions.add(definition);
                
                if ( ((String)definition.get("name")).equals(comparingFieldName) )
                    comparingFieldType= Integer.parseInt((String)definition.get("dataType"));
            }
            
            if ( comparingFieldName != null && comparingFieldName.equals("rowid") )
                comparingFieldType = MetadataDataType.STRING.getValue();            
        }
        catch(Exception e) {
             throw e;
        }
    }
     
    private String getColumnNameListStr()
    {
        StringBuilder str = new StringBuilder();
        
        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            for ( Map<String,Object> map: databaseItemDefinitions )
                str.append("\"").append(map.get("name")).append("\"").append(",");
        }
        else
        {
            for ( Map<String,Object> map: databaseItemDefinitions )
                str.append(map.get("name")).append(",");
        }

        String listStr = str.toString();
        
        return listStr.substring(0, listStr.length()-1);
    }
             
    private String generateQuerySQLForFullSync(long startLine,int pageSize,String newFilter) // 1,1000; 1001,1000
    {
        String querySql = "";
        String schema1 = schema;
        String databaseItem1 = databaseItem;
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));
            
            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
        
        isPagenated = false;
        
        if ( newFilter == null || newFilter.trim().isEmpty() )
            querySql = String.format("select * from %s%s",schema1,databaseItem1);
        else
            querySql = String.format("select * from %s%s where %s",schema1,databaseItem1,newFilter);
        
        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            querySql = String.format("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM ( %s ) A WHERE ROWNUM <= %d ) WHERE RN >= %d",querySql,startLine+pageSize-1, startLine);
            isPagenated = true;
        }
        else
        if ( databaseType == DatabaseType.MYSQL )
        {
            querySql = String.format("%s limit %d,%d",querySql,startLine-1,pageSize);
            isPagenated = true;
        }   
        else
        if ( databaseType == DatabaseType.GREENPLUM )
        {
            querySql = String.format("%s offset %d limit %d",querySql,startLine-1,pageSize);
            isPagenated = true;
        }  

        return querySql;
    }
    
    private String generateQuerySQLForFullSyncCount(String newFilter)
    {
        String sql;
        String schema1 = schema;
        String databaseItem1 = databaseItem;
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));
            
            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
        
        if ( newFilter == null || newFilter.trim().isEmpty() )
            sql = String.format("select count(*) from %s%s",schema1,databaseItem1);
        else
            sql = String.format("select count(*) from %s%s where %s",schema1,databaseItem1,newFilter);

        return sql;
    }
      
    private String getNextProcessedItems()
    {
        String startItem;
        
        if ( task.getLastProcessedItem() == null || task.getLastProcessedItem().trim().isEmpty() )
            startItem = comparingFieldValue;
        else 
            startItem = task.getLastProcessedItem();
       
        if ( startItem == null || startItem.trim().isEmpty() || startItem.equals("null") )
        {
            if ( comparingFieldType == MetadataDataType.DATE.getValue() || comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
                startItem = "1970-01-01 00:00:00";
            else    
                startItem = "0";
        }
       /* else
        {
            if ( comparingFieldType == MetadataDataType.DATE.getValue()) // +1 day
            {
                Date date = Tool.convertStringToDate(startItem, "yyyy-MM-dd HH:mm:ss");
                date = Tool.dateAddDiff(date, 24, Calendar.HOUR);
                startItem = Tool.convertDateToTimestampString(date);
            }
            else            
            if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() ) // +1 second
            {
                Date date = Tool.convertStringToDate(startItem, "yyyy-MM-dd HH:mm:ss");
                date = Tool.dateAddDiff(date, 1, Calendar.SECOND);
                startItem = Tool.convertDateToTimestampString(date);
            }
            else
            {
                long k = Long.parseLong(startItem)+1;
                startItem = String.valueOf(k);
            }
        }*/
         
        return startItem;
    }
    
      //     if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
      //              sql = String.format("select count(*) from %s%s where %s > %s",schema,databaseItem,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd"));
      //          else
      //              sql = String.format("select count(*) from %s%s where %s > '%s'",schema,databaseItem,comparingFieldName,startItem);
                    
    private String generateNewRecordQuerySQL()
    {
        String sql;
        String schema1 = schema;
        String databaseItem1 = databaseItem;
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));
            
            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
                
        if ( filter==null || filter.trim().isEmpty() )
        {
            if ( comparingFieldType == MetadataDataType.STRING.getValue() )
                sql = String.format("select %s from %s%s where %s > '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName );
            else
            if ( comparingFieldType == MetadataDataType.DATE.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select %s from %s%s where %s >= %s order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItem,"yyyy-MM-dd"), comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s >= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName );
            }
            else
            if (  comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select %s from %s%s where %s > %s and %s <= %s order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItem,"yyyy-MM-dd hh24:mi:ss"), comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItemPlus,"yyyy-MM-dd hh24:mi:ss"),comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s > '%s' and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName, lastProcessedItemPlus,comparingFieldName );
            }            
            else
                sql = String.format("select %s from %s%s where %s > %s and %s <= %s order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName,lastProcessedItemPlus,comparingFieldName);        
        }
        else
        {
            if ( comparingFieldType == MetadataDataType.STRING.getValue() )
                sql = String.format("select %s from %s%s where %s >= '%s' and ( %s ) order by %s",columnNameListStr,schema,databaseItem, comparingFieldName, lastProcessedItem, filter, comparingFieldName );
            else 
            if ( comparingFieldType == MetadataDataType.DATE.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select %s from %s%s where %s > %s and ( %s ) order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItem,"yyyy-MM-dd"), filter,comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s > '%s' and ( %s ) order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, filter,comparingFieldName );
            }
            else
            if (  comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select %s from %s%s where %s > %s and %s <= %s and ( %s ) order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItem,"yyyy-MM-dd hh24:mi:ss"),comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItemPlus,"yyyy-MM-dd hh24:mi:ss"), filter,comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s > '%s' and %s <= '%s' and ( %s ) order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName, lastProcessedItemPlus, filter,comparingFieldName );
            }            
            else
                sql = String.format("select %s from %s%s where %s > %s and %s <= %s and ( %s ) order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName, lastProcessedItemPlus,filter,comparingFieldName);              
        }

        return sql;
    }
    
    private String generateNewRecordQuerySQLForString(DatabaseType databaseType,long startLine,int pageSize,boolean isFirstTime)
    {
        String sql;
        String schema1 = schema;
        String databaseItem1 = databaseItem;
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));
            
            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
      
        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            if ( isFirstTime )
            {
                if ( filter==null || filter.trim().isEmpty() )
                    sql = String.format("select %s from %s%s where %s >'%s' and rownum<=%d and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, pageSize, comparingFieldName,maxValue,comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s >'%s' and ( %s ) and rownum<=%d and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, filter,pageSize,comparingFieldName,maxValue,comparingFieldName);              
            }
            else
            {
                if ( filter==null || filter.trim().isEmpty() )
                    sql = String.format("select %s from %s%s where %s >='%s' and rownum<=%d and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, pageSize, comparingFieldName,maxValue,comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s >='%s' and ( %s ) and rownum<=%d and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, filter,pageSize,comparingFieldName,maxValue,comparingFieldName);              
            }
        }
        else
        {
            if ( isFirstTime )
            {
                if ( filter==null || filter.trim().isEmpty() )
                    sql = String.format("select %s from %s%s where %s >'%s' and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName,maxValue,comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s >'%s' and ( %s ) and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, filter,comparingFieldName,maxValue,comparingFieldName);              
            }
            else
            {
                if ( filter==null || filter.trim().isEmpty() )
                    sql = String.format("select %s from %s%s where %s >='%s' and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, comparingFieldName,maxValue,comparingFieldName );
                else
                    sql = String.format("select %s from %s%s where %s >='%s' and ( %s ) and %s <= '%s' order by %s",columnNameListStr,schema1,databaseItem1, comparingFieldName, lastProcessedItem, filter, comparingFieldName,maxValue,comparingFieldName);              
            } 
        }
     
                        
       /* if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            sql = String.format("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM ( %s ) A WHERE ROWNUM <= %d )WHERE RN >= %d",sql,startLine+pageSize-1, startLine);
        }
        else*/
        if ( databaseType == DatabaseType.MYSQL )
        {
            sql = String.format("%s limit %d,%d",sql,startLine-1,pageSize);
        }
        else
        if ( databaseType == DatabaseType.GREENPLUM )
        {
            sql = String.format("%s offset %d limit %d",sql,startLine-1,pageSize);
        }
        
        return sql;
    }
    
    private String getLastProcessedItemsForRowid()
    {
        String lastItem;
         
        lastItem = task.getLastProcessedItem();
       
        if ( lastItem == null || lastItem.trim().isEmpty() || lastItem.equals("null") )
            lastItem = "0";
        
        return lastItem;
    }
        
    private String generateNewRecordQuerySQLCount()
    {
        String sql;
        String startItem;
        String schema1 = schema;
        String databaseItem1 = databaseItem;
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));

            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
        
        startItem = getNextProcessedItems();
       
        if ( filter==null || filter.trim().isEmpty() ) //String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value.substring(0, 19));
        {
            if ( comparingFieldType == MetadataDataType.STRING.getValue() )
                sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s>'%s'",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem);
            else 
            if ( comparingFieldType == MetadataDataType.DATE.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd"));
                else
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > '%s'",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem);
            }
            else
            if (  comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd hh24:mi:ss"));
                else
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > '%s'",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem);
            }            
            else
                sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem);
        }
        else
        {
             if ( comparingFieldType == MetadataDataType.STRING.getValue() )
                sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s>'%s' and %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem,filter);
            else 
            if ( comparingFieldType == MetadataDataType.DATE.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > %s and %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd"),filter);
                else
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > '%s' and %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem,filter);
            }
            else
            if (  comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) )
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > %s and %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,Tool.getOralceDatetimeFormat(startItem,"yyyy-MM-dd hh24:mi:ss"),filter);
                else
                    sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > '%s' and %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem,filter);
            }            
            else
                sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s > %s and %s",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem,filter);
        }

        return sql;
    }
    
    private String generateNewRecordQuerySQLCountForString()
    {
        String sql;
        String startItem;
        String schema1 = schema;
        String databaseItem1 = databaseItem;
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));

            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
        
        startItem = getNextProcessedItems();
       
        if ( filter==null || filter.trim().isEmpty() ) //String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value.substring(0, 19));
        {
            sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s>'%s'",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem);
        }
        else
        {
            sql = String.format("select count(*),min(%s),max(%s) from %s%s where %s>'%s' and ( %s )",comparingFieldName,comparingFieldName,schema1,databaseItem1,comparingFieldName,startItem,filter);
        }

        return sql;
    }
    
    private String generateNewRecordQuerySQLCountByRowid()
    {
        String sql;
        String startItem;
        
       if ( task.getLastProcessedItem() == null || task.getLastProcessedItem().trim().isEmpty() )
            startItem = comparingFieldValue;
        else
            startItem = task.getLastProcessedItem();
       
        if ( startItem == null || startItem.trim().isEmpty() || startItem.equals("null") )
        {
            startItem = "0";
        }
        
        if ( filter==null || filter.trim().isEmpty() )
        {
            sql = String.format("select count(*) from %s%s where rowid > '%s'",schema,databaseItem,startItem);
        }
        else
        {
            sql = String.format("select count(*) from %s%s where rowid > '%s' and %s",schema,databaseItem,startItem, filter);     
        }

        return sql;
    }
  
    private String getDBDriver(DatasourceConnection datasourceConnection) throws Exception
    {
        String databaseTypeStr = null;
        
        try
        {
            SAXReader saxReader = new SAXReader();
            ByteArrayInputStream in = new ByteArrayInputStream(datasourceConnection.getProperties().getBytes("utf-8"));
            Document propertiesXml= saxReader.read(in);

            List<Element> nodes = propertiesXml.selectNodes("//datasourceConnectionProperties/property");

            for( Element node: nodes)
            {
                if ( node.element("name").getTextTrim().equals("databaseType") )
                {
                    databaseTypeStr = (String)node.element("value").getTextTrim();
                    break;
                }
            }
            
            if ( databaseTypeStr == null )
                throw new Exception("2222");
                        
            return DatabaseType.findByValue(Integer.parseInt(databaseTypeStr)).getDriver();
        }
        catch(Exception e)
        {
            throw e;
        }
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
    protected void collectDataobjectContentMetadata(DataService.Client dataService,Datasource datasource,Job job, DataobjectInfo dataobject, List<String> contentIds, List<ByteBuffer> contentBinaries, List<String> contentLocations, List<Boolean> needToExtractContentTexts,String archiveFile) throws Exception
    {
        FrontendMetadata metadata;
        List<FrontendMetadata> metadataList = dataobject.getMetadataList();
             
        for(int i=0;i<contentIds.size();i++)
        {
            String contentId = contentIds.get(i);
            ByteBuffer contentBinary = contentBinaries.get(i);
            String contentLocation = contentLocations.get(i);
            boolean needToExtractContentText = needToExtractContentTexts.get(i);
            
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
    
    @Override
    public void generateIndexMetadata(List<Map<String,Object>> metadataDefinitions,Map<String, Object> jsonMap, List<FrontendMetadata> metadataList, Dataobject obj,Map<String,Object> contentInfo,boolean needEncryption) throws Exception 
    {
        generateIndexMetadataDefault(metadataDefinitions,jsonMap,metadataList,obj,contentInfo,needEncryption);
        
        if ( contentInfo.get("contentBinaries") != null )
            jsonMap.put("contents", generateContentData(obj,contentInfo,metadataList));
    }

    @Override
    protected int getDataobjectTypeId(DataobjectInfo obj) 
    {
        return targetDataobjectTypeId;
    }

    @Override
    public boolean validateDataobject(DiscoveredDataobjectInfo obj) {
        throw new UnsupportedOperationException("Not supported yet."); // no need for db processor
    }

    private String getAbsoluteFileLink(String fileLink) 
    {
        String absoluteFileLink = fileLink;
        DatasourceConnection dc;
        
        int k = fileLink.indexOf(':');
        
        if ( k>0 )
        {
            String driverName = fileLink.substring(0, k+1);
            String path = fileLink.substring(k+1);

            try
            {
                dc = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(job.getOrganizationId(),driverName).array());
                absoluteFileLink =  String.format("%s%s",dc.getLocation(),path);
            }
            catch(Exception e) {
            }
        }
        else
        if ( fileLink.startsWith("//") || fileLink.startsWith("\\\\") )
        {
            // return directly
        }
        else
        if ( fileLink.startsWith("/") || fileLink.startsWith("\\") )
        {
            String driverName = "Default:";

            try
            {
                dc = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(job.getOrganizationId(),driverName).array());
                absoluteFileLink =  String.format("%s%s",dc.getLocation(),fileLink);
            }
            catch(Exception e) {
            }     
        }

        return absoluteFileLink;
    }
 
    private String getNonWindowsAccessFileLink(String fileLink) 
    {
        String accessFileLink;
        DatasourceConnection dc;
        String mountFolder;
   
        accessFileLink = fileLink;

        int k = fileLink.indexOf(':');
        
        if ( k>0 )
        {
            String driverName = fileLink.substring(0, k+1);
            String path = fileLink.substring(k+1);

            try
            {
                dc = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(job.getOrganizationId(),driverName).array());
                mountFolder = Util.mountDatasourceConnectionFolder(dc); 
                accessFileLink =  String.format("%s:%s%s",driverName,mountFolder,path);
            }
            catch(Exception e) {
            }
        }
        else
        if ( fileLink.startsWith("//") || fileLink.startsWith("\\\\") )
        {
            // return directly
        }
        else
        if ( fileLink.startsWith("/") || fileLink.startsWith("\\") )
        {
            String driverName = "Default:";

            try
            {
                dc = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(job.getOrganizationId(),driverName).array());
                mountFolder = Util.mountDatasourceConnectionFolder(dc); 
                accessFileLink =  String.format("%s:%s%s",dc.getName(),mountFolder,fileLink);           
            }
            catch(Exception e) {
            }     
        }

        return accessFileLink;
    }    
 
    private String getSmbAbsoluteFileLink(String fileLink) 
    {
        String absoluteFileLink = fileLink;
        DatasourceConnection dc;
        String location;

        int k = fileLink.indexOf(':');
        
        if ( k>0 )
        {
            String driverName = fileLink.substring(0, k+1);
            String path = fileLink.substring(k+1);

            try
            {
                dc = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(job.getOrganizationId(),driverName).array());
                
                location = dc.getLocation();
                if ( location.startsWith("//") || location.startsWith("\\\\") )
                    location = location.substring(2);
                
                if ( dc.getUserName()==null || dc.getUserName().isEmpty() )
                    absoluteFileLink =  String.format("smb://%s%s",location,path);
                else
                    absoluteFileLink =  String.format("smb://%s:%s@%s%s",dc.getUserName(),dc.getPassword(),location,path);
            }
            catch(Exception e) {
            }
        }
        else
        if ( fileLink.startsWith("//") || fileLink.startsWith("\\\\") )
        {
            // return directly
        }
        else
        if ( fileLink.startsWith("/") || fileLink.startsWith("\\") )
        {
            String driverName = "Default:";

            try
            {
                dc = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(job.getOrganizationId(),driverName).array());
                
                location = dc.getLocation();
                if ( location.startsWith("//") || location.startsWith("\\\\") )
                    location = location.substring(2);
                
                if ( dc.getUserName()==null || dc.getUserName().isEmpty() )
                    absoluteFileLink =  String.format("smb://%s%s",location,fileLink);
                else
                    absoluteFileLink =  String.format("smb://%s:%s@%s%s",dc.getUserName(),dc.getPassword(),location,fileLink);                
            }
            catch(Exception e) {
            }     
        }

        return absoluteFileLink;
    }    
    
    @Override
    protected void getConfig(DataService.Client dataService,Datasource datasource,Job job) throws Exception
    {
       // for child class to over write
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
                        break;
                    case "lastProcessedItemDelay":
                        lastProcessedItemDelay = 0;
                        
                        try
                        {
                            lastProcessedItemDelay = Integer.parseInt(value);
                        }
                        catch(Exception e)
                        {
                        }
                        break;
                }
                
                log.info("lastProcessedItemDelay="+lastProcessedItemDelay);
            }
        }
        catch (Exception e) {
             throw e;
        }
    }
}