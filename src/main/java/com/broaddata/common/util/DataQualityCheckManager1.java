/*
 * DataQualityCheckManager.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.Date;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.commons.lang.exception.ExceptionUtils;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.commons.codec.binary.Base64;

import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.enumeration.JobItemProcessingStatisticType;
import com.broaddata.common.model.organization.DataQualityCheckJob;
import com.broaddata.common.model.organization.DataQualityCheckTask;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.DataQualityCheckDefinition;
import com.broaddata.common.model.vo.DataQualityCheckConfig;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.DataQualityCheckActionType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.vo.DataQualityCheckColumnInfo;
import java.sql.SQLException;

public class DataQualityCheckManager1 
{      
    static final Logger log = Logger.getLogger("DataQualityCheckManager");      
     
    public static final int PAGE_SIZE = 100000; // 10万
    static final int FETCH_SIZE = 1000;
        
    private DatabaseType databaseType;
    private DatabaseType resultDBDatabaseType;
    private int databaseItemType;
    private String schema;
    private String resultDBSchema;
    private String databaseItem;
    private String primaryKeyName;
    private String querySQL;
    private String countQuerySQL;
    private String filter;
    private long count;
    private String minValue,maxValue;
    private Date minDate,maxDate;
  
    private Connection dbConn = null;
    private Connection resultDbConn = null;
    private PreparedStatement statement = null;
    private PreparedStatement resultStatement = null;
    private String correctDataTableName;
    private String wrongDataTableName;
    private PreparedStatement correctDataStatement = null;
    private PreparedStatement wrongDataStatement = null;
    private ResultSet resultSet;
                
    private DataServiceConnector dsConn;
    private DataService.Client dataService;
    private DataQualityCheckJob job;
    private DataQualityCheckTask task;
    
    private boolean needPagenation;
    private Map<String,Long> duplicatedDataMap = null;
    
    public DataQualityCheckManager1(DataServiceConnector dsConn,DataService.Client dataService,DataQualityCheckJob job,DataQualityCheckTask task) 
    {
        this.dsConn = dsConn;
        this.dataService = dataService;
        this.job = job;
        this.task = task;
    }
        
    public void checkDatabaseItemData(DataQualityCheckDefinition definition) throws Exception
    {           
        int pageSize = PAGE_SIZE;
        int processed = 0;
        List<Map<String,String>> dataset; 
        Map<String,String> columnInfoMap = new HashMap<>();
        List<String> duplicationCheckColumns;
        Date checkTime = new Date();
        String tableName;
        String summaryTableName;
        String targetUser;
        
        log.info(" start check data() ....");
        
        try
        {
            duplicatedDataMap = null;
            
            if ( task.getDataQualityCheckActionType() == null )
                task.setDataQualityCheckActionType(0);
            
            DataQualityCheckConfig dataQualityCheckConfig = (DataQualityCheckConfig)Tool.deserializeObject(definition.getConfig());
            
            duplicationCheckColumns = dataQualityCheckConfig.getDuplicationCheckColumns();
          
            // creat target table
            DatasourceConnection resultDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),Integer.parseInt(task.getResultExportDatasource())).array());
            resultDbConn = JDBCUtil.getJdbcConnection(resultDatasourceConnection);
            resultDbConn.setAutoCommit(false);
            log.info("result dbConn = "+resultDbConn);
            
            resultDBSchema = Util.getDatabaseSchema(resultDatasourceConnection);
            String databaseTypeStr = Util.getDatasourceConnectionProperty(resultDatasourceConnection, "database_type");
            resultDBDatabaseType = DatabaseType.findByValue(Integer.parseInt(databaseTypeStr));
                   
            String tableSpace=Util.getDatasourceConnectionProperty(resultDatasourceConnection, "table_space");
            String indexSpace=Util.getDatasourceConnectionProperty(resultDatasourceConnection, "index_space");
            targetUser = resultDatasourceConnection.getUserName();
                 
            // create data_quality_check_result
            DataobjectType resultDataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(CommonKeys.DATAOBJECT_TYPE_FOR_DATA_QUALITY_CHECK_RESULT).array());
            List<String> primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(resultDataobjectType.getMetadatas());
            Map<String,String> columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(resultDataobjectType.getMetadatas());
      
            List<String> tableColumns = new ArrayList<>();

            List<Map<String, Object>> metadataInfoList = new ArrayList<>();
            Util.getSingleDataobjectTypeMetadataDefinition(metadataInfoList,resultDataobjectType.getMetadatas());

            for(Map<String,Object> fDefinition : metadataInfoList)
            {
                String name = (String)fDefinition.get(CommonKeys.METADATA_NODE_NAME);
                String description = (String)fDefinition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                tableColumns.add(name+"-"+description);
            }
                       
            if ( task.getResultExportName() == null || task.getResultExportName().trim().isEmpty() )
                tableName = "data_quality_check_result";
            else
                tableName = String.format("data_quality_check_result_%s",task.getResultExportName());
            
            try
            {
                if ( JDBCUtil.isTableExists(resultDbConn, resultDBSchema, tableName) == false )
                {
                    List<String> sqlList = Util.createTableForDataobjectType(false,true,resultDataobjectType.getName(),resultDataobjectType.getDescription(),tableName,"",primaryKeyNames,resultDBDatabaseType,"",metadataInfoList,tableColumns,columnLenPrecisionMap,tableSpace,indexSpace);

                    for(String createTableSql : sqlList)
                    {
                        log.info(" exceute create table sql ="+createTableSql);
                        resultStatement = resultDbConn.prepareStatement(createTableSql);
                        resultStatement.executeUpdate();
                    }
                }
            }
            catch(Exception e)
            {
                log.error("111111111 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                try{
                    resultDbConn.commit();
                }
                catch(Throwable eee){
                }
            }
            
            try
            {
                String deleteSql = String.format("delete from %s%s where check_task_id=%d and check_job_id=%d and definition_id=%d",resultDBSchema,tableName,task.getId(),job.getId(),definition.getId());
                resultStatement = resultDbConn.prepareStatement(deleteSql);
                resultStatement.executeUpdate();
                resultDbConn.commit();
            }
            catch(Exception e)
            {
                log.error("222222 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                try{
                    resultDbConn.commit();
                }
                catch(Throwable eee){
                }
            }
              
            // create data_quality_check_result_summary table
            
            DataobjectType resultSummaryDataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(CommonKeys.DATAOBJECT_TYPE_FOR_DATA_QUALITY_CHECK_RESULT_SUMMARY).array());
            primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(resultSummaryDataobjectType.getMetadatas());
            columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(resultSummaryDataobjectType.getMetadatas());
      
            tableColumns = new ArrayList<>();

            metadataInfoList = new ArrayList<>();
            Util.getSingleDataobjectTypeMetadataDefinition(metadataInfoList,resultSummaryDataobjectType.getMetadatas());

            for(Map<String,Object> fDefinition : metadataInfoList)
            {
                String name = (String)fDefinition.get(CommonKeys.METADATA_NODE_NAME);
                String description = (String)fDefinition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                tableColumns.add(name+"-"+description);
            }
    
            if ( task.getResultExportName() == null || task.getResultExportName().trim().isEmpty() )
                summaryTableName = "data_quality_check_result_summary";
            else
                summaryTableName = String.format("data_quality_check_result_summary_%s",task.getResultExportName());
            
            try
            {
                if ( JDBCUtil.isTableExists(resultDbConn, resultDBSchema, summaryTableName) == false )
                {
                    List<String> sqlList = Util.createTableForDataobjectType(false,true,resultSummaryDataobjectType.getName(),resultSummaryDataobjectType.getDescription(),summaryTableName,"",primaryKeyNames,resultDBDatabaseType,"",metadataInfoList,tableColumns,columnLenPrecisionMap,tableSpace,indexSpace);

                    for(String createTableSql : sqlList)
                    {
                        log.info(" exceute create table sql ="+createTableSql);
                        resultStatement = resultDbConn.prepareStatement(createTableSql);
                        resultStatement.executeUpdate();
                    }
                }
             }
            catch(Exception e)
            {
                log.error("33333333 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                try{
                    resultDbConn.commit();
                }
                catch(Throwable eee){
                }
            }
            
            try
            { 
                String deleteSql = String.format("delete from %s%s where check_task_id=%d and check_job_id=%d and definition_id=%d",resultDBSchema,summaryTableName,task.getId(),job.getId(),definition.getId());
                resultStatement = resultDbConn.prepareStatement(deleteSql);
                resultStatement.executeUpdate();
                resultDbConn.commit();
            }
            catch(Exception e)
            {
                log.error("44444444444 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                try{
                    resultDbConn.commit();
                }
                catch(Throwable eee){
                }
            }
            
            // connect to data source
            DatasourceConnection datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnection(job.getOrganizationId(),dataQualityCheckConfig.getTargetDatabaseId()).array());
            dbConn = JDBCUtil.getJdbcConnection(datasourceConnection);
            log.info("dbConn = "+dbConn);
                 
            databaseItemType = dataQualityCheckConfig.getTargetDatabaseItemType();
            databaseItem = dataQualityCheckConfig.getTargetDatabaseItem();
            filter = dataQualityCheckConfig.getFilter();
            primaryKeyName = getPrimaryKeyName(dataQualityCheckConfig);
            
            databaseTypeStr = Util.getDatasourceConnectionProperty(datasourceConnection, "database_type");
            databaseType = DatabaseType.findByValue(Integer.parseInt(databaseTypeStr));
            
            schema = Util.getDatabaseSchema(datasourceConnection);
                      
            Date startTime = new Date();
            
            countQuerySQL = generateCountQuerySQL();
            count = getQueryCount(countQuerySQL);

            long timeTaken = new Date().getTime()-startTime.getTime();
            log.info("execute count query, count="+count+" took ["+timeTaken+"] ms"+ " sql="+countQuerySQL);

            dataService.updateClientJob(ClientJobType.DATA_QUALITY_CHECK_JOB.getValue(), job.getOrganizationId(), job.getId(), 0, "",JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), (int)count,0,"");
            dataService.updateClientJob(ClientJobType.DATA_QUALITY_CHECK_JOB.getValue(), job.getOrganizationId(), job.getId(), 0, "",JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 0,0,"");
           
            if ( count == 0 )
            {
                log.info(" no data, return immediately!");
                return; // no new data, return
            }

            if ( task.getDataQualityCheckActionType() == DataQualityCheckActionType.SPLIT_DATA_INTO_GOOD_AND_BAD.getValue() ) // create good/bad data table
            {
                Map<String,Object> tableInfoMap = Util.getTableInfoFromDB(dbConn,null,databaseItem);
                      
                primaryKeyNames = (List<String>)tableInfoMap.get("primaryKeyColumnNames");
                metadataInfoList = (List<Map<String,Object>>)tableInfoMap.get("metadataInfoList");
                tableColumns = (List<String>)tableInfoMap.get("tableColumns");
                columnLenPrecisionMap = (Map<String,String>)tableInfoMap.get("columnLenPrecisionMap");

                correctDataTableName = String.format("%s_correct",databaseItem);
                
                try
                {      
                    if ( JDBCUtil.isTableExists(resultDbConn, resultDBSchema, correctDataTableName) == false )
                    {
                        //List<String> sqlList = Util.generateCreateTableSqlFromConnMetadata(dbConn,null,false,databaseItem,correctDataTableName,resultDBDatabaseType, tableSpace, indexSpace ,targetUser);
                        List<String> sqlList = Util.createTableForDataobjectType(false,true,"","",correctDataTableName,"",primaryKeyNames,resultDBDatabaseType,"",metadataInfoList,tableColumns,columnLenPrecisionMap,tableSpace,indexSpace);

                        for(String createTableSql : sqlList)
                        {
                            log.info(" exceute create table sql ="+createTableSql);
                            resultStatement = resultDbConn.prepareStatement(createTableSql);
                            resultStatement.executeUpdate();
                        }

                        resultDbConn.commit();
                    }
                }
                catch(Exception e)
                {
                    log.error("5555555 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                    try{
                        resultDbConn.commit();
                    }
                    catch(Throwable eee){
                    }
                }
                 
                try
                {
                    String deleteSql = String.format("delete from %s%s ",resultDBSchema,correctDataTableName);
                    resultStatement = resultDbConn.prepareStatement(deleteSql);
                    resultStatement.executeUpdate();
                    resultDbConn.commit();
                }
                catch(Exception e)
                {
                    log.error("66666666666666666666 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                    try{
                        resultDbConn.commit();
                    }
                    catch(Throwable eee){
                    }
                }
                
                wrongDataTableName = String.format("%s_wrong",databaseItem);
                
                try
                {
                    if ( JDBCUtil.isTableExists(resultDbConn, resultDBSchema, wrongDataTableName) == false )
                    {
                        List<String> sqlList = Util.createTableForDataobjectType(false,true,"","",wrongDataTableName,"",primaryKeyNames,resultDBDatabaseType,"",metadataInfoList,tableColumns,columnLenPrecisionMap,tableSpace,indexSpace);

                        for(String createTableSql : sqlList)
                        {
                            log.info(" exceute create table sql ="+createTableSql);
                            resultStatement = resultDbConn.prepareStatement(createTableSql);
                            resultStatement.executeUpdate();
                        }

                        resultDbConn.commit();
                    }
                }
                catch(Exception e)
                {
                    log.error("777777777777777777 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                    try{
                        resultDbConn.commit();
                    }
                    catch(Throwable eee){
                    }
                }
                
                try
                {
                    String deleteSql = String.format("delete from %s%s ",resultDBSchema,wrongDataTableName);
                    resultStatement = resultDbConn.prepareStatement(deleteSql);
                    resultStatement.executeUpdate();
                    resultDbConn.commit();
                }
                catch(Exception e)
                {
                    log.error("8888888888888888888 database operation failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                
                    try{
                        resultDbConn.commit();
                    }
                    catch(Throwable eee){
                    }
                }
            }
                        
            int page = 1;
                
            long totalRows = 0;
            long correctRows = 0;
            long hasProblemRows = 0;
            long duplicatedRows = 0;
            int columnSize = 0;
            List<String> columnNames = new ArrayList<>();
            Map<String,String> columnCommentsMap = new HashMap<>();
            String columnNamesStr = ""; //aaa-中文,bbb-中文
            String columnDatas = ""; //columnData;columndata;...
            long[][] columnData; // nullValueCount,1-null-is-not-allowed,2-smaller_ther,3-,...9-...
                        
            for(DataQualityCheckColumnInfo columnInfo : dataQualityCheckConfig.getColumnInfoList())
            {
                if ( !columnInfo.isNeedToCheckQuality() )
                {
                    log.info(" skip column "+columnInfo.getColumnName());
                    continue;
                }
 
                String columnName = columnInfo.getColumnName();
                String columnDescription = columnInfo.getColumnDescription();
                
                String str = String.format("%s-%s",columnName,columnDescription);
                columnNamesStr += str +",";
                columnCommentsMap.put(columnName,str);

                columnNames.add(columnName);
                columnSize++;
            }           
            
            log.info(" columnSize="+columnSize+" schema="+schema);
            
            columnNamesStr = Tool.removeLastChars(columnNamesStr, 1);
                  
            columnData = new long[columnSize][10];
            
            for(int i=0;i<columnSize;i++)
            {
                for(int j=0;j<10;j++)
                    columnData[i][j] = 0;
            }
                    
            int keyNo = 0;
            
            while(true)
            {                  
                long startLine = (page-1)*PAGE_SIZE + 1;
                querySQL = generateQuerySQL(databaseType,startLine,pageSize,databaseItem);

                log.info(String.format(" querySql = %s ",querySQL));

                statement = dbConn.prepareStatement(querySQL);
                statement.setFetchSize(1000);

                resultSet = JDBCUtil.executeQuery(statement);

                if ( resultSet.next() == false )
                {
                    log.info(" no more result!");
                    break;
                }
                else
                {                            
                    if ( needPagenation )
                        page++;
                    
                    dataset = new ArrayList<>(); 
                     
                    ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
                    int colNum = resultSetMetadata.getColumnCount();   
                                                                                 
                    do
                    {
                        Map<String,String> rowMap = new HashMap<>();
                        
                        String value = "";

                        for (int k = 1; k <= colNum; k++)
                        {
                            String type = resultSetMetadata.getColumnTypeName(k);
                            String columnName = resultSetMetadata.getColumnName(k);
                            
                            //log.info(" column type="+type);

                            if ( columnName.equals("RN") )
                                continue;

                            if ( type.equals("DATE") ) 
                            {
                                if (resultSet.getDate(columnName) == null) 
                                    value = "1970-01-01";
                                else 
                                if (resultSet.getTime(columnName) == null)
                                    value = resultSet.getDate(columnName) + "";
                                else
                                    value = resultSet.getDate(columnName) + " " + resultSet.getTime(columnName);
                            } 
                            else 
                            if ( type.equals("BLOB") )
                            {
                                Blob b = resultSet.getBlob(columnName);
                                value =  new String(Base64.encodeBase64(JDBCUtil.getBlobString(b)),"UTF-8");
                            } 
                            else 
                            if ( type.equals("CLOB") ) 
                            {
                                Clob b = resultSet.getClob(columnName);
                                value =  new String(Base64.encodeBase64(JDBCUtil.getClobString(b)), "UTF-8");
                            }
                            else 
                            if ( type.equals("VARCHAR") || type.equals("CHAR") || type.equals("VARCHAR2") ) 
                            {
                                value = resultSet.getString(columnName);
                            } 
                            else
                                value = resultSet.getString(columnName);
                                
                                //value = FileUtil.isNullToChar(resultSet.getString(columnName));

                            rowMap.put(columnName, value);
                            
                            if ( processed == 0 )
                            {
                                columnInfoMap.put(columnName, type);  
                            }
                        }

                        processed++;

                        dataset.add(rowMap);
                    }
                    while(resultSet.next());   

                    // get group by data for duplication test
                    if ( !duplicationCheckColumns.isEmpty() && duplicatedDataMap==null )
                    {                        
                        String columnStr = "";
                    
                        for(String cName : duplicationCheckColumns)
                            columnStr += cName+",";
                                              
                        columnStr = Tool.removeLastChars(columnStr, 1);
                       
                        String groupBySql = "";
                        
                        if ( filter == null || filter.trim().isEmpty() )
                        {
                            if ( schema.isEmpty() )
                                groupBySql = String.format("select %s,count(*) from %s group by %s",columnStr,databaseItem,columnStr);
                            else
                                groupBySql = String.format("select %s,count(*) from %s%s group by %s",columnStr,schema,databaseItem,columnStr);
                        }
                        else
                        {
                            if ( schema.isEmpty() )
                                groupBySql = String.format("select %s,count(*) from %s where %s group by %s",columnStr,databaseItem,filter,columnStr);
                            else
                                groupBySql = String.format("select %s,count(*) from %s%s where %s group by %s",columnStr,schema,databaseItem,filter,columnStr);
                        }

                        statement = dbConn.prepareStatement(groupBySql);
                        resultSet = JDBCUtil.executeQuery(statement);
                        
                        duplicatedDataMap = new HashMap<>();
                        
                        while(resultSet.next())
                        {                            
                            long count = resultSet.getLong(duplicationCheckColumns.size()+1);
                            String columnValue = "";
                            
                            for(String cName : duplicationCheckColumns)
                                columnValue += resultSet.getString(cName)+";";
                            
                            columnValue = Tool.removeLastChars(columnValue, 1);
                            
                            duplicatedDataMap.put(columnValue,count);
                        }
                    }
                    
                    // check data
                                 
                    String insertSql = String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?)",tableName);
    
                    resultStatement = resultDbConn.prepareStatement(insertSql);
                    
                    if ( task.getDataQualityCheckActionType() == DataQualityCheckActionType.SPLIT_DATA_INTO_GOOD_AND_BAD.getValue() )
                    {
                        String questionStr = "";
                        for(int q=0;q<dataQualityCheckConfig.getColumnInfoList().size();q++)
                            questionStr += "?,";
                        
                        questionStr = Tool.removeLastChars(questionStr, 1);
                         
                        String insertWrongDataSql = String.format("INSERT INTO %s VALUES (%s)",wrongDataTableName,questionStr);
                        wrongDataStatement = resultDbConn.prepareStatement(insertWrongDataSql);
                          
                        String insertCorrectDataSql = String.format("INSERT INTO %s VALUES (%s)",correctDataTableName,questionStr);
                        correctDataStatement = resultDbConn.prepareStatement(insertCorrectDataSql);
                    }
             
                    int i=0;
              
                    for(Map<String,String> row : dataset)
                    {
                        totalRows++;
                        int isCorrect;
                        String errorInfo;
                        String primaryKeyValue = "";
                        //String allData = "";
                        String nullValueColumnNames = "";
                        
                        i++;
                        
                        if ( !dataQualityCheckConfig.isHasPrimaryKey() )
                        {
                            keyNo++;
                            primaryKeyValue = String.format("%d,",keyNo); 
                        }
                                                
                        JSONObject jsonObject= new JSONObject();
                        JSONArray array = new JSONArray();
                        jsonObject.put("dataQualityCheckColumnCheckInfo", array);
                    
                        int j=0;
                        int k = -1;
                        
                        String duplicationCheckColumnNameStr = "";
                        String duplicationCheckColumnValueStr = "";
                   
                        for(DataQualityCheckColumnInfo columnInfo : dataQualityCheckConfig.getColumnInfoList())
                        {    
                            if ( !columnInfo.isNeedToCheckQuality() )
                            {
                                log.info(" skip column "+columnInfo.getColumnName());
                                continue;
                            }
                            
                            k++;
                            String columnValue = row.get(columnInfo.getColumnName());
                            
                            if ( columnValue == null || columnValue.isEmpty() )
                            {
                                columnData[k][0]++;
                                nullValueColumnNames += columnInfo.getColumnName() +",";
                            }
                            
                            if ( dataQualityCheckConfig.isHasPrimaryKey() )
                            {
                                if ( columnInfo.isIsPrimaryKey() )
                                    primaryKeyValue += columnValue+",";
                            }
                            else
                            {
                                if ( columnInfo.isSelfPointedPrimaryKey())
                                    primaryKeyValue += columnValue+",";
                            }
                            
                            //allData += columnValue+",";             
                                      
                            if ( columnInfo.isIsDuplicationCheckColumn() )
                            {
                                duplicationCheckColumnNameStr += columnInfo.getColumnName()+";";
                                
                                String str = columnValue;
                                
                                if ( str == null )
                                    str = "";
                                    
                                duplicationCheckColumnValueStr += str +";";
                            }
 
                            String error = checkColumn(columnValue,columnInfo,columnData[k]);
                            
                            if ( error.isEmpty() )
                                continue;
                                                        
                            JSONObject columnObject = new JSONObject();
                            columnObject.put("checkError",error);
                            columnObject.put("columnValue", columnValue);
                            columnObject.put("columnName", columnInfo.getColumnName());
                                                      
                            array.put(j,columnObject);
                            j++;
                        }
                        
                        if ( !duplicationCheckColumns.isEmpty() )
                        {   
                            duplicationCheckColumnValueStr = Tool.removeLastChars(duplicationCheckColumnValueStr, 1);
                            
                            Long val = duplicatedDataMap.get(duplicationCheckColumnValueStr);
                            
                            if ( val == null )
                                count = 0;
                            else
                                count = val;
                            
                            log.info(" count="+count+", duplicationCheckColumnValueStr="+duplicationCheckColumnValueStr);
                            
                        /*   statement = dbConn.prepareStatement(duplicationCheckSql);
                            resultSet = JDBCUtil.executeQuery(statement);
                            resultSet.next();

                            long count = resultSet.getInt(1);*/
            
                            if ( count > 1 )
                            {
                                duplicatedRows++;
                                
                                JSONObject columnObject = new JSONObject();          
                                columnObject.put("checkError", Util.getErrorMessage("data_quality_check_error_data_duplicated",duplicationCheckColumnNameStr,duplicationCheckColumnValueStr));                                
                                columnObject.put("columnValue", duplicationCheckColumnValueStr);
                                columnObject.put("columnName", duplicationCheckColumnNameStr);

                                array.put(j,columnObject);
                                j++;
                            }
                        }
                        
                        primaryKeyValue = Tool.removeLastChars(primaryKeyValue, 1);
                        
                        log.info(" finish checking... page="+(page-1)+" No.="+i+" primarykey="+primaryKeyValue);
                                                
                        if ( !nullValueColumnNames.isEmpty() )
                            nullValueColumnNames = Tool.removeLastChars(nullValueColumnNames, 1);
                                
                        //if ( !primaryKeyValue.isEmpty() )
                        //    primaryKeyValue = Tool.removeLastChars(primaryKeyValue, 1);
                        //else
                        //    primaryKeyValue = Tool.removeLastChars(allData, 1);
                                   
                        if ( j == 0 )
                        {
                            isCorrect = 1;
                            errorInfo = "";
                            
                            correctRows++;
                        }
                        else
                        {
                            isCorrect = 0;
                            errorInfo = Tool.formatJson(jsonObject.toString());
                            
                            hasProblemRows++;
                        }
                   
                        resultStatement.setObject(1, task.getId(), MetadataDataType.INTEGER.getSqlType());  // CHECK_TASK_ID
                        resultStatement.setObject(2, job.getId(), MetadataDataType.INTEGER.getSqlType());
                        resultStatement.setObject(3, definition.getId(), MetadataDataType.INTEGER.getSqlType());
                        resultStatement.setObject(4, dataQualityCheckConfig.getTargetDatabaseItem(), MetadataDataType.STRING.getSqlType());
                        resultStatement.setObject(5, primaryKeyValue,  MetadataDataType.STRING.getSqlType());
                        resultStatement.setObject(6, errorInfo,  MetadataDataType.STRING.getSqlType());
                        resultStatement.setObject(7, nullValueColumnNames,  MetadataDataType.STRING.getSqlType());
                        resultStatement.setObject(8, isCorrect, MetadataDataType.INTEGER.getSqlType());
                        resultStatement.setObject(9, checkTime, MetadataDataType.TIMESTAMP.getSqlType());
                        
                        resultStatement.addBatch();
                        
                        if ( task.getDataQualityCheckActionType() == DataQualityCheckActionType.SPLIT_DATA_INTO_GOOD_AND_BAD.getValue() )
                        {
                            if ( isCorrect == 1 )
                            {
                                int kk = 0;
                                for(DataQualityCheckColumnInfo columnInfo : dataQualityCheckConfig.getColumnInfoList())
                                {       
                                    kk++;
                                    int dataType = columnInfo.getEDFDataType();
                                    String columnValue = row.get(columnInfo.getColumnName());
                                 
                                    Object object = Util.getValueObject(columnValue,dataType);
                                    correctDataStatement.setObject(kk, object, MetadataDataType.findByValue(dataType).getSqlType());
                                }
                                
                                correctDataStatement.addBatch();
                            }
                            else
                            {
                                int kk = 0;
                                for(DataQualityCheckColumnInfo columnInfo : dataQualityCheckConfig.getColumnInfoList())
                                {       
                                    kk++;
                                    int dataType = columnInfo.getEDFDataType();
                                    String columnValue = row.get(columnInfo.getColumnName());
                                 
                                    Object object = Util.getValueObject(columnValue,dataType);
                                    wrongDataStatement.setObject(kk, object, MetadataDataType.findByValue(dataType).getSqlType());
                                }
                                
                                wrongDataStatement.addBatch();
                            }
                        }
                    }
                    
                    try
                    {
                        resultStatement.executeBatch();

                        if ( task.getDataQualityCheckActionType() == DataQualityCheckActionType.SPLIT_DATA_INTO_GOOD_AND_BAD.getValue() )
                        {
                            wrongDataStatement.executeBatch();
                            correctDataStatement.executeBatch();

                            JDBCUtil.close(wrongDataStatement);
                            JDBCUtil.close(correctDataStatement);
                        }

                        resultDbConn.commit(); 
                    }
                    catch(SQLException e)
                    {                        
                        log.error(" execute batch failed! e="+e+",cause="+e.getNextException()+", stack trace="+ExceptionUtils.getStackTrace(e));
                        throw e;
                    }
                    
                    JDBCUtil.close(resultSet);
                    JDBCUtil.close(statement);
                    
                    dataService.updateClientJob(ClientJobType.DATA_QUALITY_CHECK_JOB.getValue(), job.getOrganizationId(), job.getId(), 0, "",JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processed,0,"");
           
                    log.info("11111111 needPagenation = "+needPagenation);
                    
                    if ( !needPagenation )
                        break;
                }
            }
            
             // creat columnDatas
            columnDatas = "";

            for(int ii=0;ii<columnSize;ii++)
            {
                String columnDataStr = columnCommentsMap.get(columnNames.get(ii))+",";

                for(int jj=0;jj<10;jj++)
                {
                    columnDataStr += String.valueOf(columnData[ii][jj]) +",";
                }

                columnDataStr = Tool.removeLastChars(columnDataStr, 1);

                columnDatas += columnDataStr + ";";
            }

            columnDatas = Tool.removeLastChars(columnDatas, 1);

            String insertSummarySql = String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?)",summaryTableName);

            resultStatement = resultDbConn.prepareStatement(insertSummarySql);
            resultStatement.setObject(1, task.getId(), MetadataDataType.INTEGER.getSqlType());  // CHECK_TASK_ID
            resultStatement.setObject(2, job.getId(), MetadataDataType.INTEGER.getSqlType());
            resultStatement.setObject(3, definition.getId(), MetadataDataType.INTEGER.getSqlType());
            resultStatement.setObject(4, dataQualityCheckConfig.getTargetDatabaseItem(), MetadataDataType.STRING.getSqlType());
            resultStatement.setObject(5, totalRows,  MetadataDataType.LONG.getSqlType());
            resultStatement.setObject(6, correctRows,  MetadataDataType.LONG.getSqlType());
            resultStatement.setObject(7, duplicatedRows,  MetadataDataType.LONG.getSqlType());
            resultStatement.setObject(8, hasProblemRows,  MetadataDataType.LONG.getSqlType());
            resultStatement.setObject(9, columnNamesStr, MetadataDataType.STRING.getSqlType());
            resultStatement.setObject(10, columnDatas, MetadataDataType.STRING.getSqlType());
            resultStatement.setObject(11, checkTime, MetadataDataType.TIMESTAMP.getSqlType());

            resultStatement.executeUpdate();

            resultDbConn.commit();            
        }
        catch (Exception e)
        {
            String errorStr = " processRecord() failed! countQuerySQL=["+countQuerySQL+"] querySQL="+querySQL+" e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            throw new Exception(errorStr);
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(statement);
            JDBCUtil.close(dbConn);
            
            JDBCUtil.close(resultStatement);
            JDBCUtil.close(resultDbConn);
        }  
    }
    
    private Map<String,Long> getDuplicatedDataMap()
    {
        Map<String,Long> dataMap = new HashMap<>();
        
        
        
        
        
        return dataMap;
    }

    private String getPrimaryKeyName(DataQualityCheckConfig dataQualityCheckConfig)
    {
        for( DataQualityCheckColumnInfo info : dataQualityCheckConfig.getColumnInfoList() )
        {
            if ( info.isIsPrimaryKey() )
                return info.getColumnName();
        }
        
        return "";
    }
    
    private String checkColumn(String columnValue,DataQualityCheckColumnInfo columnInfo,long[] columnData)
    {
        String columnName;
        int dataType;
        StringBuilder result = new StringBuilder();
         
        try 
        {
            columnName = columnInfo.getColumnName();
            dataType = columnInfo.getEDFDataType();
           
            if ( columnInfo.isNullIsAllowed() == false ) // check null
            {
                if ( columnValue == null || columnValue.isEmpty() )
                {
                    result.append(Util.getErrorMessage("data_quality_check_error_null_is_not_allowed",columnName));
                    columnData[1]++;
                    return result.toString();
                }
            }
            
            if ( columnValue == null || columnValue.isEmpty() )
                return "";
            
            if ( dataType == MetadataDataType.STRING.getValue() )
            {
                if ( columnInfo.getMinValue() != null && !columnInfo.getMinValue().isEmpty() )
                {
                    if ( columnValue.compareTo(columnInfo.getMinValue()) < 0 )
                    {
                        result.append(Util.getErrorMessage("data_quality_check_error_smaller_than_min_value",columnName,columnValue,columnInfo.getMinValue())).append(";");
                        columnData[2]++;
                    }
                }
                
                if ( columnInfo.getMaxValue() != null && !columnInfo.getMaxValue().isEmpty() )
                {
                    if ( columnValue.compareTo(columnInfo.getMaxValue()) > 0 )
                    {
                        result.append(Util.getErrorMessage("data_quality_check_error_bigger_than_max_value",columnName,columnValue,columnInfo.getMaxValue())).append(";");
                        columnData[3]++;
                    }
                }
                                            
                if ( columnInfo.getRegularExpression() != null && !columnInfo.getRegularExpression().trim().isEmpty() )
                {
                    Pattern pattern  = Pattern.compile(columnInfo.getRegularExpression().trim());
                    boolean matched = pattern.matcher(columnValue).matches();
                    
                    if ( matched == false )
                    {
                        result.append(Util.getErrorMessage("data_quality_check_error_not_match_regular_expression",columnName,columnValue,columnInfo.getRegularExpression().trim())).append(";");   
                        columnData[6]++;
                    }
                }
            }
            else
            if ( dataType == MetadataDataType.INTEGER.getValue() || dataType == MetadataDataType.LONG.getValue() ||
                    dataType == MetadataDataType.DOUBLE.getValue() || dataType == MetadataDataType.FLOAT.getValue() )
            {                
                if ( dataType == MetadataDataType.INTEGER.getValue() )
                {
                    int intVal = 0;

                    try
                    {
                        intVal = Integer.parseInt(columnValue);
                    }
                    catch(Exception e) {
                    }

                    if ( columnInfo.getMinValue() != null && !columnInfo.getMinValue().isEmpty() )
                    {
                        int minValue = 0;

                        try {
                            minValue = Integer.parseInt(columnInfo.getMinValue());
                        }
                        catch(Exception e) {
                        }

                        if ( intVal < minValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_smaller_than_min_value",columnName,columnValue,columnInfo.getMinValue())).append(";");
                            columnData[2]++;
                        }
                    }

                    if ( columnInfo.getMaxValue() != null && !columnInfo.getMaxValue().isEmpty() )
                    {
                        int maxValue = 0;

                        try {
                            maxValue = Integer.parseInt(columnInfo.getMaxValue());
                        }
                        catch(Exception e) {
                        }

                        if ( intVal > maxValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_bigger_than_max_value",columnName,columnValue,columnInfo.getMaxValue())).append(";");
                            columnData[3]++;
                        }
                    }
                }
                else
                if ( dataType == MetadataDataType.LONG.getValue() )
                {
                    long longVal = 0;

                    try
                    {
                        longVal = Long.parseLong(columnValue);
                    }
                    catch(Exception e) {
                    }

                    if ( columnInfo.getMinValue() != null && !columnInfo.getMinValue().isEmpty() )
                    {
                        long minValue = 0;

                        try {
                            minValue = Long.parseLong(columnInfo.getMinValue());
                        }
                        catch(Exception e) {
                        }

                        if ( longVal < minValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_smaller_than_min_value",columnName,columnValue,columnInfo.getMinValue())).append(";");
                            columnData[2]++;
                        }
                    }

                    if ( columnInfo.getMaxValue() != null && !columnInfo.getMaxValue().isEmpty() )
                    {
                        long maxValue = 0;

                        try {
                            maxValue = Long.parseLong(columnInfo.getMaxValue());
                        }
                        catch(Exception e) {
                        }

                        if ( longVal > maxValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_bigger_than_max_value",columnName,columnValue,columnInfo.getMaxValue())).append(";");
                            columnData[3]++;
                        }
                    }
                }
                else
                if ( dataType == MetadataDataType.FLOAT.getValue() )
                {
                    float floatVal = 0;

                    try
                    {
                        floatVal = Float.parseFloat(columnValue);
                    }
                    catch(Exception e) {
                    }

                    if ( columnInfo.getMinValue() != null && !columnInfo.getMinValue().isEmpty() )
                    {
                        float minValue = 0;

                        try {
                            minValue = Float.parseFloat(columnInfo.getMinValue());
                        }
                        catch(Exception e) {
                        }

                        if ( floatVal < minValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_smaller_than_min_value",columnName,columnValue,columnInfo.getMinValue())).append(";");
                            columnData[2]++;
                        }
                    }

                    if ( columnInfo.getMaxValue() != null && !columnInfo.getMaxValue().isEmpty() )
                    {
                        float maxValue = 0;

                        try {
                            maxValue = Float.parseFloat(columnInfo.getMaxValue());
                        }
                        catch(Exception e) {
                        }

                        if ( floatVal > maxValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_bigger_than_max_value",columnName,columnValue,columnInfo.getMaxValue())).append(";");
                            columnData[3]++;
                        }
                    }
                }
                else
                if ( dataType == MetadataDataType.DOUBLE.getValue() )
                {
                    double doubleVal = 0;

                    try
                    {
                        doubleVal = Double.parseDouble(columnValue);
                    }
                    catch(Exception e) {
                    }

                    if ( columnInfo.getMinValue() != null && !columnInfo.getMinValue().isEmpty() )
                    {
                        double minValue = 0;

                        try {
                            minValue = Double.parseDouble(columnInfo.getMinValue());
                        }
                        catch(Exception e) {
                        }

                        if ( doubleVal < minValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_smaller_than_min_value",columnName,columnValue,columnInfo.getMinValue())).append(";");
                            columnData[2]++;
                        }
                    }

                    if ( columnInfo.getMaxValue() != null && !columnInfo.getMaxValue().isEmpty() )
                    {
                        double maxValue = 0;

                        try {
                            maxValue = Double.parseDouble(columnInfo.getMaxValue());
                        }
                        catch(Exception e) {
                        }

                        if ( doubleVal > maxValue )
                        {
                            result.append(Util.getErrorMessage("data_quality_check_error_bigger_than_max_value",columnName,columnValue,columnInfo.getMaxValue())).append(";");
                            columnData[3]++;
                        }
                    }
                }      
                
                if ( columnInfo.getRegularExpression() != null && !columnInfo.getRegularExpression().trim().isEmpty() )
                {
                    Pattern pattern  = Pattern.compile(columnInfo.getRegularExpression().trim());
                    boolean matched = pattern.matcher(columnValue).matches();
                    
                    if ( matched == false )
                    {
                        result.append(Util.getErrorMessage("data_quality_check_error_not_match_regular_expression",columnName,columnValue,columnInfo.getRegularExpression().trim())).append(";");
                        columnData[6]++;
                    }
                }
            }
            else    
            if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue())
            {
                Date dateValue = null;
                String minValueDateStr = "";
                String maxValueDateStr = "";
                
                try
                {
                    if ( dataType == MetadataDataType.DATE.getValue() )
                    {
                        dateValue = Tool.convertDateStringToDateAccordingToPattern(columnValue, "yyyy-MM-dd");
                        minValueDateStr = Tool.convertDateToString(columnInfo.getMinValueDate(), "yyyy-MM-dd");
                        maxValueDateStr = Tool.convertDateToString(columnInfo.getMaxValueDate(), "yyyy-MM-dd");
                    }
                    else
                    {
                        dateValue = Tool.convertDateStringToDateAccordingToPattern(columnValue, "yyyy-MM-dd HH:mm:ss");
                        minValueDateStr = Tool.convertDateToString(columnInfo.getMinValueDate(), "yyyy-MM-dd HH:mm:ss");
                        maxValueDateStr = Tool.convertDateToString(columnInfo.getMaxValueDate(), "yyyy-MM-dd HH:mm:ss");
                    }
                }
                catch(Exception e) {
                }

                if ( columnInfo.getMinValueDate() != null )
                {
                    if ( dateValue == null || dateValue.before(columnInfo.getMinValueDate()) )
                    {
                        result.append(Util.getErrorMessage("data_quality_check_error_smaller_than_min_value",columnName,columnValue,minValueDateStr)).append(";");
                        columnData[2]++;
                    }
                }
                
                if ( columnInfo.getMaxValueDate() != null )
                {
                    if ( dateValue != null && dateValue.after(columnInfo.getMaxValueDate()) )
                    {
                        result.append(Util.getErrorMessage("data_quality_check_error_bigger_than_max_value",columnName,columnValue,maxValueDateStr)).append(";");
                        columnData[3]++;
                    }
                }
            }
            else
                return "";
                                                  
            if ( columnInfo.getMinLen() > 0 && dataType != MetadataDataType.DATE.getValue() && dataType != MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( columnValue.length() < columnInfo.getMinLen() )
                {
                    result.append(Util.getErrorMessage("data_quality_check_error_len_smaller_than_min_len",columnName,columnValue,columnInfo.getMinLen())).append(";");
                    columnData[4]++;
                }
            }

            if ( columnInfo.getMaxLen() > 0 && dataType != MetadataDataType.DATE.getValue() &&  dataType != MetadataDataType.TIMESTAMP.getValue() )
            {
                if ( columnValue.length() > columnInfo.getMaxLen() )
                {
                    result.append(Util.getErrorMessage("data_quality_check_error_len_bigger_than_max_len",columnName,columnValue,columnInfo.getMaxLen())).append(";");
                    columnData[5]++;
                }
            }
            
            if ( columnInfo.getValueScope()!= null && !columnInfo.getValueScope().trim().isEmpty() && dataType != MetadataDataType.DATE.getValue() &&  dataType != MetadataDataType.TIMESTAMP.getValue() )
            {
                String[] vals = columnInfo.getValueScope().trim().split("\\,");

                boolean found = false;
                for(String val : vals)
                {
                    if ( val.equals(columnValue) )
                    {
                        found = true;
                        break;
                    }
                }

                if ( !found )
                {
                    result.append(Util.getErrorMessage("data_quality_check_error_not_in_value_scope",columnName,columnValue,columnInfo.getValueScope())).append(";");
                    columnData[7]++;
                }
            }   
             
            if ( dataType == MetadataDataType.STRING.getValue() || dataType == MetadataDataType.INTEGER.getValue() || dataType == MetadataDataType.LONG.getValue() )
            {
                if ( columnInfo.getCodeDefinitionId() != null && !columnInfo.getCodeDefinitionId().trim().isEmpty() )
                {
                    String key = "";

                    try {
                        key = dataService.getCodeValue(job.getOrganizationId(),Integer.parseInt(columnInfo.getCodeDefinitionId().trim()),columnValue);
                    }
                    catch(Exception e)
                    {
                        log.warn(" dataService.getCodeValue() failed! codeId="+columnInfo.getCodeDefinitionId()+" codeValue="+columnValue);
                    }

                    if ( key.isEmpty() )
                    {
                        result.append(Util.getErrorMessage("data_quality_check_error_not_in_code_values",columnName,columnValue,columnInfo.getCodeDefinitionId())).append(";");
                        columnData[8]++;
                    }
                }           
            }
                         
            return result.toString();
        }
        catch(Exception e)
        {
            log.info(" checkColumn() failed! e="+e);
            throw e;
        }
    }
       
    private String generateDuplicationCheckSql()
    {
        String querySql;

        if ( schema.isEmpty() )
            querySql = String.format("select count(*) from %s",databaseItem);
        else
            querySql = String.format("select count(*) from %s%s",schema,databaseItem);
          
        return querySql;
    }
    
    private String generateQuerySQL(DatabaseType databaseType,long startLine,int pageSize,String tableName) // 1,1000; 1001,1000
    {
        String querySql;

        needPagenation = false;

        if ( filter == null || filter.trim().isEmpty() )
        {
            if ( schema.isEmpty() )
                querySql = String.format("select * from %s",tableName);
            else
                querySql = String.format("select * from %s%s",schema,tableName);
        }
        else
        {
            if ( schema.isEmpty() )
                querySql = String.format("select * from %s where %s",tableName,filter);
            else
                querySql = String.format("select * from %s%s where %s",schema,tableName,filter);
        }

        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            querySql = String.format("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM ( %s ) A WHERE ROWNUM <= %d )WHERE RN >= %d",querySql,startLine+pageSize-1, startLine);
            needPagenation = true;
        }
        else
        if ( databaseType == DatabaseType.MYSQL )
        {
            querySql = String.format("%s limit %d,%d",querySql,startLine-1,pageSize);
            needPagenation = true;
        }
        else
        if ( databaseType == DatabaseType.GREENPLUM )
        {
            querySql = String.format("%s order by %s offset %d limit %d",querySql,primaryKeyName,startLine-1,pageSize);
            needPagenation = true;
        }
              
        return querySql;
    }
    
    private String generateCountQuerySQL() // 1,1000; 1001,1000
    {
        String querySql = "";

        needPagenation = false;

        if ( filter == null || filter.trim().isEmpty() )
        {
            if ( schema.isEmpty() )
                querySql = String.format("select count(*) from %s",databaseItem);
            else
                querySql = String.format("select count(*) from %s%s",schema,databaseItem);
        }
        else
        {
            if ( schema.isEmpty() )
                querySql = String.format("select count(*) from %s where %s",databaseItem,filter);
            else
                querySql = String.format("select count(*) from %s%s where %s",schema,databaseItem,filter);
        }
 
        return querySql;
    }
        
    private long getQueryCount(String countQuerySQL) throws Exception
    {
        try 
        {
            log.info(" countQuerySQL ="+countQuerySQL);
            
            statement = dbConn.prepareStatement(countQuerySQL);
            resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();
 
            long count = resultSet.getInt(1);
                                   
            log.info(" count="+count+" minValue="+minValue+" maxValue="+maxValue+" minDate"+minDate+" maxDate="+maxDate);
            
            return count;
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
}
