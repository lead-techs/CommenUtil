/*
 * DataExportUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap; 
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.sql.Connection;
import java.sql.Statement;
import com.itextpdf.text.Document;
import com.itextpdf.text.Font;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfWriter;
import java.util.Date;
import java.sql.SQLException;
import javax.persistence.EntityManager;
import org.json.JSONObject;
import java.io.OutputStreamWriter;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import java.nio.ByteBuffer;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.broaddata.common.model.enumeration.FileType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.RowSelectType;
import com.broaddata.common.model.enumeration.SearchType;
import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.AnalyticQuery;
import com.broaddata.common.model.organization.DataExportTask;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import com.broaddata.common.thrift.dataservice.SearchResponse;
import com.broaddata.common.model.enumeration.DataviewType;
import com.broaddata.common.thrift.dataservice.ExportJob;
import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.enumeration.DatabaseUpdateMode;
import com.broaddata.common.model.enumeration.FileTypeForExport;
import com.broaddata.common.model.enumeration.SearchScope;
import com.broaddata.common.model.organization.DataSubscriber;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.thrift.dataservice.Dataset;

public class DataExportUtil 
{
    static final Logger log = Logger.getLogger("DataExportUtil");

    public static List<DataSubscriber> getSubscribers(EntityManager em,int organizationId,int dataSubscriptionId) throws Exception 
    {
        String sql;
        List<DataSubscriber> list = new ArrayList<>();
        
        try
        {
            sql = String.format("from DataSubscriber where organizationId=%d and dataSubscriptionId=%d",organizationId,dataSubscriptionId);
            list = em.createQuery(sql).getResultList();
        } 
        catch (Exception e) {
            throw e;
        }
  
        return list;
    }
        
    public static Map<String,Object> retrieveDataviewData(int organizationId,DataService.Client dataService,AnalyticDataview dataview,AnalyticQuery query, int searchFrom, int maxExpectedHits, int maxExecutionTime) throws Exception
    {
        SearchRequest request = null;
        SearchResponse response = null;
    
        Map<String,Object> searchResult = new HashMap<>();
        List<DataobjectVO> dataobjects;
        
        int repositoryId = 0; 
        int catalogId = 0;
        
        String indexType;
               
        try
        {
            repositoryId = Integer.parseInt(query.getTargetRepositories());
            catalogId = Integer.parseInt(query.getTargetCatalogs());
            
            indexType = query.getTargetIndexTypes();

            if ( dataview.getType() ==  DataviewType.NORMAL.getValue() )
            {
                request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_GET_ALL_RESULTS.getValue());

                request.setTargetIndexKeys(null);
                request.setTargetIndexTypes(Arrays.asList(indexType));
                request.setQueryStr(query.getQueryString());
                request.setFilterStr(query.getFilterString());
                
                request.setSearchFrom(dataview.getStartRow()-1);
                request.setMaxExpectedHits(dataview.getExpectedRowNumber());         
                
                request.setMaxExecutionTime(maxExecutionTime);
                request.setColumns(dataview.getColumns());
                request.setSortColumns(dataview.getSortColumns());

                response = dataService.getDataobjects(request);
                
                dataobjects = Util.generateDataobjectsForDataset(response.getSearchResults(),true);
                
                searchResult.put("totalHits",(long)response.getSearchResults().size());
                searchResult.put("currentHits",response.getSearchResults().size());
                searchResult.put("dataobjectVOs", dataobjects);
            }
            else
            {
                if ( dataview.getTimeAggregationColumn() == null || dataview.getTimeAggregationColumn().trim().isEmpty() )
                {
                    request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_FOR_AGGREGATION.getValue());

                    request.setTargetIndexKeys(null);
                    request.setTargetIndexTypes(Arrays.asList(indexType));
                    request.setQueryStr(query.getQueryString());
                    request.setFilterStr(query.getFilterString());

                    request.setSearchFrom(0);
                    request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);    

                    request.setMaxExecutionTime(maxExecutionTime);
                    request.setGroupby(dataview.getGroupBy());
                    request.setAggregationColumns(dataview.getAggregationColumns());
                    request.setOrderby(dataview.getOrderBy());

                    response = dataService.getDataobjects(request);
                }
                else  // has time aggregation
                {
                    // generate time period list
                    Date queryStartTime = Util.getQueryTime(null,null,"startTime",dataService,dataview,organizationId,repositoryId,catalogId,indexType); 
                    Date queryEndTime = Util.getQueryTime(null,null,"endTime",dataService,dataview,organizationId,repositoryId,catalogId,indexType); 
                    
                    List<Map<String,Object>> periodList = Util.getTimePeriod(dataview,queryStartTime,queryEndTime);

                    List<Map<String,String>> results = new ArrayList<>();
                    
                    for(Map<String,Object> period : periodList )
                    {
                        Date startTime = (Date)period.get("startTime");
                        
                        String timeRangeStr = (String)period.get("timeRangeStr");
                        String queryStr = query.getQueryString();
                        
                        request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_FOR_AGGREGATION.getValue());

                        request.setTargetIndexKeys(null);
                        request.setTargetIndexTypes(Arrays.asList(indexType));
                        request.setQueryStr(String.format("(%s) AND (%s)",queryStr,timeRangeStr));
                        request.setFilterStr(query.getFilterString());

                        request.setSearchFrom(0);
                        request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);    

                        request.setMaxExecutionTime(maxExecutionTime);
                        request.setGroupby(dataview.getGroupBy());
                        request.setAggregationColumns(dataview.getAggregationColumns());
                        request.setOrderby(dataview.getOrderBy());

                        response = dataService.getDataobjects(request);
 
                        List<Map<String,String>> result = response.getSearchResults();
                        
                        Util.addTimeRangeForDatasetAggregation(result,startTime,dataview.getTimeAggregationColumn());
                        
                        results.addAll(result);
                    }
                    
                    response.setSearchResults(results);
                }
                              
                if ( dataview.getAggregationRowSelectType() == RowSelectType.TOP.getValue() )
                {
                    List<Map<String,String>> result = response.getSearchResults();
                    
                    int expectedRowNumber = dataview.getAggregationExpectedRowNumber();
                    
                    if ( expectedRowNumber < result.size() )
                    {
                        int diff = result.size() - expectedRowNumber;
                        
                        for( int i=0;i<diff;i++ )
                            result.remove(result.size()-1);
                        
                        response.setSearchResults(result);
                    } 
                }        
                else
                if ( dataview.getAggregationRowSelectType() == RowSelectType.RANGE.getValue() )
                {
                    List<Map<String,String>> newResult = new ArrayList<>();
                    List<Map<String,String>> result = response.getSearchResults();
                    
                    for(int i=0;i<result.size();i++)
                    {
                        if ( i >= (dataview.getAggregationStartRow()-1) && i < (dataview.getAggregationStartRow()-1)+dataview.getAggregationExpectedRowNumber() )
                            newResult.add(result.get(i));
                        
                        if ( i == (dataview.getAggregationStartRow()-1)+dataview.getAggregationExpectedRowNumber() )
                            break;
                    }         
                        
                    response.setSearchResults(newResult);
                }   
                
                dataobjects = Util.generateDataobjectsForDatasetAggregation(response.getSearchResults());
                
                searchResult.put("totalHits",(long)response.getSearchResults().size());
                searchResult.put("currentHits", response.getSearchResults().size());
                searchResult.put("dataobjectVOs", dataobjects);
            }
        }
        catch (Exception e)
        {
            throw e;
        }
          
        return searchResult;
    }
                 
    public static void exportDataviewDataToDatabase(DataService.Client dataService, DataExportTask exportDataTask, ExportJob exportJob, AnalyticDataview dataview, DatasourceConnection datasourceConnection) throws Exception
    {
        String insertSql = null;
        String updateSql = null;
        String whereSql = null;
        int retry = 0;
      
        String schema;
        String driverName;
        Connection conn = null;
        Statement stmt = null;
        String[] columns;
        Map<String,String> columnDataTypes;
        String column;
        DatabaseType databaseType;
        List<String> primaryKeyNames = null; 
        int processedItems;
        int index = 0;
        int updateNum = 0;
        String dataobjectTypeName = null;
        String tableColumnStr;
        Map<String,String> tableColumnNameMap = new HashMap<>();
        Date currentDate;
        long dateValue;
        ByteBuffer buf;
        String targetTableName = "";
        String sql;
        boolean batchProcessing = true;

        try
        {
            schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);           
            conn = JDBCUtil.getJdbcConnection(datasourceConnection);
            
            stmt = conn.createStatement();
            
            String[] vals = exportDataTask.getExportTableName().split("\\~");
  
            targetTableName = vals[0];
                    
            // create temp table sql
            databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);
            
            Map<String,String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());
            String tableSpace=properties.get("table_space");
            String indexSpace=properties.get("index_space");
            
            if ( exportDataTask.getDatabaseUpdateMode() == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue()  )
            {
                try
                {
                    if ( JDBCUtil.isTableExists(conn, schema, targetTableName) )
                    {
                        sql = String.format("drop table %s",targetTableName); log.info("11111 sql="+sql);
                        stmt.executeUpdate(sql);
                    }
                }
                catch(Exception ee)
                {
                    log.info(" drop table failed! e="+ee+" stacktrace="+ExceptionUtils.getStackTrace(ee));
                    try{
                        conn.commit();
                    }
                    catch(Throwable eee){
                    }
                }
            }
          
            try
            {
                if ( JDBCUtil.isTableExists(conn, schema, targetTableName) == false )
                {
                    List<String> createTablSqlList = dataService.getDataviewTableSQL(dataview.getOrganizationId(),dataview.getId(),exportDataTask.getExportTableName(),databaseType.getValue(),tableSpace,indexSpace);

                    for(String createTableSql : createTablSqlList)
                    {
                        //log.info("22222 sql="+createTableSql);
                        stmt.executeUpdate(createTableSql); 
                    }
                }
            }
            catch(Exception ee)
            {
                try{
                    conn.commit();
                }
                catch(Throwable eee){
                }
                log.info(" create table failed! e="+ee+" stacktrace="+ExceptionUtils.getStackTrace(ee));
            }

            columnDataTypes = dataService.getDataviewColumnTypes(dataview.getOrganizationId(),dataview.getId());
            
            dataobjectTypeName = columnDataTypes.get("dataobjectTypeName");
            log.info("333333333 dataobjectTypeName = "+dataobjectTypeName);
          
            boolean ifDataobjectTypeWithChangedColumnName = columnDataTypes.get("ifDataobjectTypeWithChangedColumnName").equals("true");
            
            primaryKeyNames = new ArrayList<>();
            
            String str = columnDataTypes.get("primaryKeyFieldNameStr");
            if ( str != null )
            {
                String[] strs = str.split("\\,");
                primaryKeyNames = Arrays.asList(strs);
            }
               
            if ( dataview.getType() == DataviewType.NORMAL.getValue() || dataview.getType() == DataviewType.SQL_STYLE.getValue() )
                columns = dataview.getColumns().split("\\;");
            else
            {
                if ( dataview.getTimeAggregationColumn() == null || dataview.getTimeAggregationColumn().trim().isEmpty() )
                    columns = dataview.getAggregationColumns().split("\\;");
                else
                    columns = (dataview.getTimeAggregationColumn().trim()+";"+dataview.getAggregationColumns()).split("\\;");
            }
                  
            buf = ByteBuffer.wrap(Tool.serializeObject(dataview));
            Dataset dataviewDataset = dataService.getDataviewData(exportDataTask.getOrganizationId(), dataview.getId(),buf,null,CommonKeys.SIZE_FOR_DATAVIEW_BATCH,600, true);
            String scrollId = dataviewDataset.getScrollId();
                        
            String columnNameStr = "";

            for(String exportColumn : columns)
            {
                tableColumnStr = Tool.changeTableColumnName(dataobjectTypeName,exportColumn,ifDataobjectTypeWithChangedColumnName);
                columnNameStr += tableColumnStr+",";
                
                tableColumnNameMap.put(exportColumn, tableColumnStr);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length()-1);
                    
            index = 0;            
            conn.setAutoCommit(false);
                            
            batchProcessing = true;
                
            while(true)
            {
                log.info("2222222222222222 get search result! number ="+dataviewDataset.getRows().size()+" columnName="+columnNameStr);
             
                Date date1 = new Date();
                log.info(" 11111111111111 start save to db!");

                processedItems = 0;
                
                for(Map<String,String> row : dataviewDataset.getRows())
                {
                    index++;
                    
                    if ( index <= exportJob.alreadyProcessItems )
                        continue;                        
                        
                    if ( exportDataTask.getDatabaseUpdateMode() == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue()  )
                    {
                        insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",targetTableName,columnNameStr);
                        updateSql = String.format("UPDATE %s SET ",targetTableName); 
                        whereSql = String.format("WHERE ");
                    }
                    else
                    {
                        insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",targetTableName,columnNameStr);
                        updateSql = String.format("UPDATE %s SET ",targetTableName); 
                        whereSql = String.format("WHERE ");
                    }
                     
                    //sql = String.format("INSERT INTO %s VALUES ( ",tempTargetTableName);

                    for(String exportColumn : columns)
                    {
                        String value = row.get(exportColumn);

                        if ( value == null )
                            value = "";
                        else
                        {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);
                            
                            if ( !value.isEmpty() && Tool.isNumber(value)  )
                                value = Tool.normalizeNumber(value);
                        }

                        //if ( value.isEmpty() )
                        //    log.info(" 777777 value is null");
                        
                        value = Tool.removeQuotation(value);
                                                
                        if ( exportColumn.startsWith("count("))
                            column = "*";
                        else
                        if ( exportColumn.contains("(") )
                            column = exportColumn.substring(exportColumn.indexOf("(")+1,exportColumn.indexOf(")"));
                        else
                            column = exportColumn;

                        int dataType = Integer.parseInt((String)columnDataTypes.get(column));

                        if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue()  )
                        {
                            if ( dataview.getType() == DataviewType.AGGREGATION.getValue() || dataview.getType() == DataviewType.NORMAL.getValue() )
                            {
                                if ( dataview.getType() == DataviewType.AGGREGATION.getValue() )
                                {
                                    try 
                                    {
                                        currentDate = Tool.changeFromGmt(new Date(Long.parseLong(value)));
                                        value = Tool.convertDateToTimestampString(currentDate);
                                    }
                                    catch(Exception e)
                                    {
                                        log.info(" no need to convert");
                                        value = value.substring(0,19);
                                    }
                                }
                                else
                                {                                             
                                    dateValue = Tool.convertESDateStringToLong(value);

                                    currentDate = Tool.changeFromGmt(new Date(dateValue));

                                    if ( dataType == MetadataDataType.DATE.getValue() )
                                        value = Tool.convertDateToDateString(currentDate);
                                    else
                                        value = Tool.convertDateToTimestampStringWithMilliSecond1(currentDate);
                                }
                            }
                       }
                
                        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE)
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                    dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                    dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if(value.isEmpty() )
                                {
                                    String clp = columnDataTypes.get(exportColumn+"-lenPrecision");
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    insertSql += String.format("'%s',",defaultNullVal);
                                }
                                    else
                                    insertSql += String.format("%s,",value);
                            }
                            else
                            if ( dataType == MetadataDataType.DATE.getValue() )
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd'),",value);
                            else
                            if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value.substring(0, 19));
                            else
                                insertSql += String.format("'%s',",value);
                        }
                        else
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                    dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                    dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if(value.isEmpty() )
                                {
                                    String clp = columnDataTypes.get(exportColumn+"-lenPrecision");
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    insertSql += String.format("'%s',",defaultNullVal);
                                }
                                    else
                                    insertSql += String.format("%s,",value);
                            }
                            else
                                insertSql += String.format("'%s',",value);
                        }
                                                
                        boolean isPrimaryKey = false;

                        for(String kName : primaryKeyNames)
                        {
                            if( kName.equals(exportColumn) )
                            {
                                isPrimaryKey = true;
                                break;
                            }
                        }

                        //tableColumnStr = Tool.changeTableColumnName(dataobjectTypeName,exportColumn);
                        tableColumnStr = tableColumnNameMap.get(exportColumn);
                        
                         if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE)
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if( value.isEmpty() )
                                {
                                    String clp = columnDataTypes.get(exportColumn+"-lenPrecision");
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);
                                 
                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = '%s' AND",tableColumnStr,defaultNullVal);
                                    else
                                        updateSql += String.format(" %s = '%s',",tableColumnStr,defaultNullVal);
                                }
                                else
                                {
                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                    else
                                        updateSql += String.format(" %s = %s,",tableColumnStr,value);
                                }
                            }
                            else
                            if ( dataType == MetadataDataType.DATE.getValue() )
                            {
                                value = String.format("TO_DATE('%s', 'yyyy-mm-dd')",value);

                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                else
                                    updateSql += String.format(" %s = %s,",tableColumnStr,value);
                            }
                            else
                            if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                            {
                                value = String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss')",value.substring(0, 19));

                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                else
                                    updateSql += String.format(" %s = %s,",tableColumnStr,value);
                            }
                            else
                            {
                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = '%s' AND",tableColumnStr,value);
                                else
                                    updateSql += String.format(" %s = '%s',",tableColumnStr,value);
                            }   
                        }
                        else
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if( value.isEmpty() )
                                {
                                    String clp = columnDataTypes.get(exportColumn+"-lenPrecision");
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = '%s' AND",tableColumnStr,defaultNullVal);
                                    else
                                        updateSql += String.format(" %s = '%s',",tableColumnStr,defaultNullVal);
                                }
                                else
                                {
                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                    else
                                        updateSql += String.format(" %s = %s,",tableColumnStr,value);
                                }
                            }
                            else
                            {
                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = '%s' AND",tableColumnStr,value);
                                else
                                    updateSql += String.format(" %s = '%s',",tableColumnStr,value);
                            }                    
                        }
                    }

                    insertSql = insertSql.substring(0,insertSql.length()-1)+")";
 
                    if ( updateSql != null )
                    {
                        updateSql = updateSql.substring(0,updateSql.length()-1);

                        if ( whereSql != null && !whereSql.trim().isEmpty() )
                            whereSql = whereSql.substring(0,whereSql.length()-3);
                        else
                            whereSql = "";

                        updateSql += " "+ whereSql;
                    }
                                
                    retry = 0;
                
                    while(true)
                    {
                        retry++;

                        try
                        {
                            if ( batchProcessing == true )
                                stmt.addBatch(insertSql);
                            else
                            {
                                stmt.executeUpdate(insertSql);
                                log.info(" insert succeed! ");
                            }
                            break;
                        }
                        catch(Exception e)
                        {
                            log.info(" insert into database failed! try update e="+e+" insertSql="+insertSql);
                            
                            try{
                                conn.commit();
                            }
                            catch(Throwable ee){
                            }

                            try
                            {
                                stmt.executeUpdate(updateSql);
                                
                                updateNum++;
                                //log.info(" update succeed! dataobjectId="+obj.getId());
                                break;
                            }
                            catch(Exception ee)
                            {
                                log.info(" update database failed! try update again e="+e+" sql="+updateSql);   
                                Tool.SleepAWhile(1, 0);

                                try {
                                    conn.commit();
                                }
                                catch(Exception eee){
                                }
                                
                                if ( retry>5 )
                                {
                                    log.error(" update database failed! e="+ee+" sql="+updateSql);  
                                    throw e;
                                }
                            }
                        }
                    }
 
                    processedItems++; 
                }
                
                if ( batchProcessing == true)
                {
                    try
                    {
                        Date startTime = new Date();
                        
                        stmt.executeBatch();
                        
                        log.info(String.format("finish execute batch!!! took %d ms, size=%d",(new Date().getTime() - startTime.getTime()),processedItems));
                    }
                    catch(Exception e)
                    {                        
                        String errorInfo = "7777777777777777 execute batch failed! e="+e;
                        log.error(errorInfo);

                        try{
                            conn.commit();
                        }
                        catch(Throwable ee){
                        }
                        
                        batchProcessing = false;
                        //JDBCUtil.close(stmt);
                        continue;
                    }
                                                        
                    conn.commit();
                }
              
                dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
                processedItems = 0;
                     
                long timeTaken1 = new Date().getTime() - date1.getTime();
                log.info(" 2222222 save to db end! size="+ dataviewDataset.getRows().size()+" take time="+ timeTaken1);
            
                if ( dataviewDataset.hasMoreData )
                {
                    retry = 0;
                    while(true)
                    {
                        retry++;
                        
                        Date date2 = new Date();
                        log.info(" 11111111111111 start retrieve data!");
                        
                        dataviewDataset = dataService.getRemainingDataviewData(exportDataTask.getOrganizationId(), dataview.getId(), buf, scrollId, 600,false);
                        
                        long timeTaken2 = new Date().getTime() - date2.getTime();
                        log.info(" 1111111111111111 retrieve data end! size="+ dataviewDataset.rows.size()+" take time="+ timeTaken2);
                        
                        if ( dataviewDataset.getRows().isEmpty() && retry<5 )
                        {
                            Tool.SleepAWhileInMS(200);
                        }
                        else
                            break;
                    }
                                                          
                    batchProcessing = true;
                }
                else
                    break;
            }
                
            conn.commit();
             
            if ( processedItems > 0 )
                dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
                            
            log.info("processed line ="+index+" updated line="+updateNum);           
        }
        catch(Exception e)
        {
            log.error(" exportDataToDatabase() failed! e="+e);
            //if ( conn != null )
            //    conn.rollback();
            
           /* if ( exportDataTask.getDatabaseUpdateMode()  == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue() )
            {
                try 
                {
                    sql = String.format("drop table %s",targetTableName);
                    stmt.executeUpdate(sql);
                }
                catch(Exception ee)
                {
                    log.error(" drop temp table failed! table="+targetTableName+" ee="+ee);
                }
            }*/

            throw e;
        }
        finally
        {
            JDBCUtil.close(stmt);
            JDBCUtil.close(conn);
        }          
    }
    
    public static void exportDataobjectTypeDataToDatabase(DataService.Client dataService, DataExportTask exportDataTask, ExportJob exportJob, AnalyticDataview dataview, AnalyticQuery query,DatasourceConnection datasourceConnection) throws Exception
    {
        String targetTableName = null;
        String tempTargetTableName = null;
        String insertSql = null;
        String updateSql = null;
        String whereSql = null;
        int retry = 0;
      
        String schema;
        String driverName;
        Connection conn = null;
        Statement stmt = null;
        String[] columns;
        Map<String,String> columnDataTypes;
        String column;
        DatabaseType databaseType;
        List<String> primaryKeyNames = null; 
        int processedItems;
        int index = 0;
        int updateNum = 0;
        String dataobjectTypeName = null;
        String tableColumnStr;
        Map<String,String> tableColumnNameMap = new HashMap<>();
        Date currentDate;
        long dateValue;
        ByteBuffer buf;
        boolean batchProcessing = true;
        int k=0;

        try
        {
            schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);           
            conn = JDBCUtil.getJdbcConnection(datasourceConnection);
            
            stmt = conn.createStatement();
            
            // get table name: organizationId_dataviewName
            targetTableName = String.format("%s",exportDataTask.getExportTableName().trim());
            tempTargetTableName = String.format("%s_%d", new Date().getTime());
            //tempTargetTableName = tempTargetTableName.replaceAll("-", "_");
            
            // create temp table sql
            databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);
            
            Map<String,String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());
            String tableSpace=properties.get("table_space");
            String indexSpace=properties.get("index_space");
            
            if ( exportDataTask.getDatabaseUpdateMode() == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue()  )
            {
                List<String> createTablSqlList = dataService.getDataviewTableSQL(dataview.getOrganizationId(),dataview.getId(),tempTargetTableName,databaseType.getValue(),tableSpace,indexSpace);
 
                for(String createTableSql : createTablSqlList)
                    stmt.executeUpdate(createTableSql);
            }
            else
            {
                if ( JDBCUtil.isTableExists(conn, schema, targetTableName) == false )
                {
                    List<String> createTablSqlList = dataService.getDataviewTableSQL(dataview.getOrganizationId(),dataview.getId(),targetTableName,databaseType.getValue(),tableSpace,indexSpace);
 
                    for(String createTableSql : createTablSqlList)
                        stmt.executeUpdate(createTableSql); 
                }
            }

            columnDataTypes = dataService.getDataviewColumnTypes(dataview.getOrganizationId(),dataview.getId());
            
            dataobjectTypeName = columnDataTypes.get("dataobjectTypeName");
            log.info("333333333 dataobjectTypeName = "+dataobjectTypeName);
          
            boolean ifDataobjectTypeWithChangedColumnName = columnDataTypes.get("ifDataobjectTypeWithChangedColumnName").equals("true");
            
            primaryKeyNames = new ArrayList<>();
            
            String str = columnDataTypes.get("primaryKeyFieldNameStr");
            if ( str != null )
            {
                String[] strs = str.split("\\,");
                primaryKeyNames = Arrays.asList(strs);
            }
               
            if ( dataview.getType() == DataviewType.NORMAL.getValue() )
                columns = dataview.getColumns().split("\\;");
            else
            {
                if ( dataview.getTimeAggregationColumn() == null || dataview.getTimeAggregationColumn().trim().isEmpty() )
                    columns = dataview.getAggregationColumns().split("\\;");
                else
                    columns = (dataview.getTimeAggregationColumn().trim()+";"+dataview.getAggregationColumns()).split("\\;");
            }
                  
            buf = ByteBuffer.wrap(Tool.serializeObject(dataview));
            Dataset dataviewDataset = dataService.getDataviewData(exportDataTask.getOrganizationId(), dataview.getId(),buf,null,CommonKeys.SIZE_FOR_DATAVIEW_BATCH,600, true);
            String scrollId = dataviewDataset.getScrollId();
                        
            String columnNameStr = "";

            for(String exportColumn : columns)
            {
                tableColumnStr = Tool.changeTableColumnName(dataobjectTypeName,exportColumn,ifDataobjectTypeWithChangedColumnName);
                columnNameStr += tableColumnStr+",";
                
                tableColumnNameMap.put(exportColumn, tableColumnStr);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length()-1);
                    
            processedItems = 0;
            index = 0;            
            conn.setAutoCommit(false);
                  
            batchProcessing = true;
                
            while(true)
            {
                log.info("2222222222222222 get search result! number ="+dataviewDataset.getRows().size()+" columnName="+columnNameStr);
             
                Date date1 = new Date();
                log.info(" 11111111111111 start save to db!");
          
                for(Map<String,String> row : dataviewDataset.getRows())
                {
                    index++;
                    
                    if ( index <= exportJob.alreadyProcessItems )
                        continue;                        
                        
                    if ( exportDataTask.getDatabaseUpdateMode() == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue()  )
                    {
                        insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",tempTargetTableName,columnNameStr);
                        updateSql = String.format("UPDATE %s SET ",tempTargetTableName); 
                        whereSql = String.format("WHERE ");
                    }
                    else
                    {
                        insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",targetTableName,columnNameStr);
                        updateSql = String.format("UPDATE %s SET ",targetTableName); 
                        whereSql = String.format("WHERE ");
                    }
                     
                    //sql = String.format("INSERT INTO %s VALUES ( ",tempTargetTableName);

                    for(String exportColumn : columns)
                    {
                        String value = row.get(exportColumn);

                        if ( value == null )
                            value = "";
                        else
                        {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);
                            
                            if ( !value.isEmpty() && Tool.isNumber(value)  )
                                value = Tool.normalizeNumber(value);
                        }

                        value = Tool.removeQuotation(value);
                        
                        if ( exportColumn.startsWith("count("))
                            column = "*";
                        else
                        if ( exportColumn.contains("(") )
                            column = exportColumn.substring(exportColumn.indexOf("(")+1,exportColumn.indexOf(")"));
                        else
                            column = exportColumn;

                        int dataType = Integer.parseInt((String)columnDataTypes.get(column));

                        if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue()  )
                        {
                            if ( dataview.getType() == DataviewType.AGGREGATION.getValue() )
                            {
                                try 
                                {
                                    currentDate = Tool.changeFromGmt(new Date(Long.parseLong(value)));
                                    value = Tool.convertDateToTimestampString(currentDate);
                                }
                                catch(Exception e)
                                {
                                    log.info(" no need to convert");
                                    value = value.substring(0,19);
                                }
                            }
                            else
                            {                                             
                                dateValue = Tool.convertESDateStringToLong(value);
                                currentDate = Tool.changeFromGmt(new Date(dateValue));
                                
                                if ( dataType == MetadataDataType.DATE.getValue() )
                                    value = Tool.convertDateToDateString(currentDate);
                                else
                                    value = Tool.convertDateToTimestampStringWithMilliSecond1(currentDate);
                            }
                        }
                
                        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE)
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                    dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                    dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if(value.isEmpty() )
                                {
                                    String clp = columnDataTypes.get(exportColumn+"-lenPrecision");
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    insertSql += String.format("'%s',",defaultNullVal);
                                }
                                    else
                                    insertSql += String.format("%s,",value);
                            }
                            else
                            if ( dataType == MetadataDataType.DATE.getValue() )
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd'),",value);
                            else
                            if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value.substring(0, 19));
                            else
                                insertSql += String.format("'%s',",value);
                        }
                        else
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                    dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                    dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if(value.isEmpty() )
                                {
                                    String clp = columnDataTypes.get(exportColumn+"-lenPrecision");
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    insertSql += String.format("'%s',",defaultNullVal);
                                }
                                    else
                                    insertSql += String.format("%s,",value);
                            }
                            else
                                insertSql += String.format("'%s',",value);
                        }
                        
                        
                        boolean isPrimaryKey = false;

                        for(String kName : primaryKeyNames)
                        {
                            if( kName.equals(exportColumn) )
                            {
                                isPrimaryKey = true;
                                break;
                            }
                        }

                        //tableColumnStr = Tool.changeTableColumnName(dataobjectTypeName,exportColumn);
                        tableColumnStr = tableColumnNameMap.get(exportColumn);
                        
                        if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                            dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                            dataType == MetadataDataType.DOUBLE.getValue() )
                        {
                            if( value.isEmpty() )
                            {
                                String clp = columnDataTypes.get(exportColumn+"-lenPrecision");
                                String defaultNullVal = Tool.getDefaultNullVal(clp);

                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = '%s' AND",tableColumnStr,defaultNullVal);
                                else
                                    updateSql += String.format(" %s = '%s',",tableColumnStr,defaultNullVal);
                            }
                            else
                            {
                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                else
                                    updateSql += String.format(" %s = %s,",tableColumnStr,value);
                            }
                        }
                        else
                        {
                            if ( isPrimaryKey )
                                whereSql += String.format(" %s = '%s' AND",tableColumnStr,value);
                            else
                                updateSql += String.format(" %s = '%s',",tableColumnStr,value);
                        }                             
                    }

                    insertSql = insertSql.substring(0,insertSql.length()-1)+")";
 
                    if ( updateSql != null )
                    {
                        updateSql = updateSql.substring(0,updateSql.length()-1);

                        if ( whereSql != null && !whereSql.trim().isEmpty() )
                            whereSql = whereSql.substring(0,whereSql.length()-3);
                        else
                            whereSql = "";

                        updateSql += " "+ whereSql;
                    }
                                
                    retry = 0;
                
                    while(true)
                    {
                        retry++;

                        try
                        {
                            if ( batchProcessing == true )
                            {
                                stmt.addBatch(insertSql);
                            }
                            else
                            {
                                stmt.executeUpdate(insertSql);
                                log.info(" insert succeed! ");
                            }
                            break;
                        }
                        catch(Exception e)
                        {
                            log.info(" insert into database failed! try update e="+e+" insertSql="+insertSql);
                               
                            try{
                                conn.commit();
                            }
                            catch(Throwable ee){
                            }

                            try
                            {
                                stmt.executeUpdate(updateSql);
                                
                                updateNum++;
                                log.info(" update succeed!");
                                break;
                            }
                            catch(Exception ee)
                            {
                                log.info(" update database failed! try update again e="+e+" sql="+updateSql);   
                                Tool.SleepAWhile(1, 0);

                                if ( retry>5 )
                                {
                                    log.error(" update database failed! e="+ee+" sql="+updateSql);  
                                    throw e;
                                }
                            }
                        }
                    }
 
                    processedItems++; 
                }
                     
                if ( batchProcessing == true)
                {
                    try
                    {
                        Date startTime = new Date();
                        
                        stmt.executeBatch();
                        
                        log.info(String.format("finish execute batch!!! took %d ms, size=%d",(new Date().getTime() - startTime.getTime()),processedItems));
                    }
                    catch(Exception e)
                    {                        
                        String errorInfo = "7777777777777777 execute batch failed! e="+e;
                        log.error(errorInfo);

                        try{
                            conn.commit();
                        }
                        catch(Throwable ee){
                        }
                        
                        batchProcessing = false;
                        //JDBCUtil.close(stmt);
                        continue;
                    }
                                                        
                    conn.commit();
                }

                dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
                processedItems = 0;

                if ( batchProcessing == false )
                    conn.commit();

                log.info(" commit to database per "+CommonKeys.JDBC_BATCH_COMMIT_NUMBER);
                         
                long timeTaken1 = new Date().getTime() - date1.getTime();
                log.info(" 2222222 save to db end! size="+ dataviewDataset.getRows().size()+" take time="+ timeTaken1);
            
                if ( dataviewDataset.hasMoreData )
                {
                    retry = 0;
                    while(true)
                    {
                        retry++;
                        
                        Date date2 = new Date();
                        log.info(" 11111111111111 start retrieve data!");
                        
                        dataviewDataset = dataService.getRemainingDataviewData(exportDataTask.getOrganizationId(), dataview.getId(), buf, scrollId, 600,false);
                        
                        long timeTaken2 = new Date().getTime() - date2.getTime();
                        log.info(" 1111111111111111 retrieve data end! size="+ dataviewDataset.rows.size()+" take time="+ timeTaken2);
                        
                        if ( dataviewDataset.getRows().isEmpty() && retry<5 )
                        {
                            Tool.SleepAWhileInMS(200);
                        }
                        else
                            break;
                    }
                    
                    batchProcessing = true;
                }
                else
                    break;
            }
                
            if ( processedItems > 0 )
                dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
            
            conn.commit();
                
            log.info("processed line ="+index+" updated line="+updateNum);
             
            retry = 0;
            
            while(true)
            {
                retry++;
                    
                try
                {
                    if ( exportDataTask.getDatabaseUpdateMode() == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue() )
                    {
                        String sql = "";
                        
                         if ( JDBCUtil.isTableExists(conn, schema, targetTableName) )
                        {
                            sql = String.format("drop table %s",targetTableName);
                            stmt.executeUpdate(sql);
                        }

                        if ( databaseType == DatabaseType.ORACLE )
                            sql=String.format("rename %s to %s",tempTargetTableName,targetTableName); 
                        else
                            sql=String.format("rename table %s to %s",tempTargetTableName,targetTableName); 
                            
                        //insertSql=String.format("rename table %s to %s",tempTargetTableName,targetTableName);
                        stmt.executeUpdate(sql);
                    }

                    break;
                }
                catch(Exception e)
                {
                    log.error(" drop and rename table failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                    Tool.SleepAWhile(1, 0);
                    
                    if ( retry>3 )
                        throw e;
                }
            }
            
            conn.commit();
        }
        catch(Exception e) 
        {
            log.error(" exportDataToDatabase() failed! e="+e);
            //if ( conn != null )
            //    conn.rollback();
            
            if ( exportDataTask.getDatabaseUpdateMode()  == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue() )
            {
                try 
                {
                    String sql = String.format("drop table %s",tempTargetTableName); log.info(" sql ="+sql);
                    stmt.executeUpdate(sql);
                }
                catch(Exception ee)
                {
                    log.error(" drop temp table failed! table="+tempTargetTableName+" ee="+ee+" stacktrace="+ExceptionUtils.getStackTrace(e));
                }
            }
                   
            throw e;
        }
        finally
        {
            JDBCUtil.close(stmt);
            JDBCUtil.close(conn);
        }          
    }
    
    public static String exportDataToTxt(DataService.Client dataService, DataExportTask exportDataTask, ExportJob exportJob, AnalyticDataview dataview, String tmpDirectory) throws Exception
    {
        String line = null;
        String[] columns;
        Map<String,String> columnDataTypes;
        String column;

        int processedItems;
        String dataobjectTypeName;
        String tableColumnStr;
        Map<String,String> tableColumnNameMap = new HashMap<>();
        Date currentDate;
        long dateValue;
        String exportFilename;
        int dataType;
        String value;
        OutputStreamWriter writer = null;
        ByteBuffer buf;

        try
        {
            exportFilename = String.format("%s/%s.%s.%s",tmpDirectory,UUID.randomUUID().toString(),exportDataTask.getExportFileName(),FileType.findByValue(exportDataTask.getExportFileType()).getExt());

            writer = new OutputStreamWriter(new FileOutputStream(exportFilename),"GBK");
             
            columnDataTypes = dataService.getDataviewColumnTypes(dataview.getOrganizationId(),dataview.getId());
            
            dataobjectTypeName = columnDataTypes.get("dataobjectTypeName");
            log.info("333333333 dataobjectTypeName = "+dataobjectTypeName);
          
            boolean ifDataobjectTypeWithChangedColumnName = columnDataTypes.get("ifDataobjectTypeWithChangedColumnName").equals("true");
                           
            if ( dataview.getType() == DataviewType.NORMAL.getValue() )
                columns = dataview.getColumns().split("\\;");
            else
            {
                if ( dataview.getTimeAggregationColumn() == null || dataview.getTimeAggregationColumn().trim().isEmpty() )
                    columns = dataview.getAggregationColumns().split("\\;");
                else
                    columns = (dataview.getTimeAggregationColumn().trim()+";"+dataview.getAggregationColumns()).split("\\;");
            }
                  
            buf = ByteBuffer.wrap(Tool.serializeObject(dataview));
            Dataset dataviewDataset = dataService.getDataviewData(exportDataTask.getOrganizationId(), dataview.getId(),buf,null,CommonKeys.SIZE_FOR_DATAVIEW_BATCH,600, true);
            String scrollId = dataviewDataset.getScrollId();
        
            String columnNameStr = "";

            for(String exportColumn : columns)
            {
                tableColumnStr = Tool.changeTableColumnName(dataobjectTypeName,exportColumn,ifDataobjectTypeWithChangedColumnName);
                columnNameStr += tableColumnStr+",";
                
                tableColumnNameMap.put(exportColumn, tableColumnStr);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length()-1);
            writer.write(columnNameStr+"\n");
                    
            processedItems = 0;          
              
            while(true)
            {
                log.info("2222222222222222 get search result! number ="+dataviewDataset.getRows().size()+" columnName="+columnNameStr);
             
                for(Map<String,String> row : dataviewDataset.getRows())
                {
                    line = "";
                    
                    for(String exportColumn : columns)
                    {
                        value = row.get(exportColumn);

                        if ( value == null || value.equals("null") )
                            value = "";
                        else
                        {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);
                            
                            value = Tool.replaceComma(value);
                            
                            if ( !value.isEmpty() && Tool.isNumber(value)  )
                                value = Tool.normalizeNumber(value);
                        }

                        value = Tool.removeQuotation(value);
                        
                        if ( exportColumn.startsWith("count("))
                            column = "*";
                        else
                        if ( exportColumn.contains("(") )
                            column = exportColumn.substring(exportColumn.indexOf("(")+1,exportColumn.indexOf(")"));
                        else
                            column = exportColumn;

                        dataType = Integer.parseInt((String)columnDataTypes.get(column));

                        if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue()  )
                        {
                            if ( dataview.getType() == DataviewType.AGGREGATION.getValue() )
                            {
                                try 
                                {
                                    currentDate = Tool.changeFromGmt(new Date(Long.parseLong(value)));
                                    value = Tool.convertDateToTimestampString(currentDate);
                                }
                                catch(Exception e)
                                {
                                    log.info(" no need to convert");
                                    value = value.substring(0,19);
                                }
                            }
                            else
                            {                                             
                                dateValue = Tool.convertESDateStringToLong(value);
                                currentDate = Tool.changeFromGmt(new Date(dateValue));
                                
                                if ( dataType == MetadataDataType.DATE.getValue() )
                                    value = Tool.convertDateToDateString(currentDate);
                                else
                                    value = Tool.convertDateToTimestampStringWithMilliSecond1(currentDate);
                            }
                        }

                        line += String.format("%s,",value);
                    }

                    line = line.substring(0,line.length()-1);
   
                    writer.write(line+"\n");
                    
                    processedItems++;
                                          
                    if ( processedItems >= CommonKeys.FILE_BATCH_COMMIT_NUMBER )
                    {
                        writer.flush();
                        
                        dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
                        processedItems = 0;
                    }
                }
            
                if ( dataviewDataset.hasMoreData )
                {
                    int retry = 0;
                    while(true)
                    {
                        retry++;
                        
                        Date date2 = new Date();
                        log.info(" 11111111111111 start retrieve data!");
                        
                        dataviewDataset = dataService.getRemainingDataviewData(exportDataTask.getOrganizationId(), dataview.getId(), buf, scrollId, 600,false);
                        
                        long timeTaken2 = new Date().getTime() - date2.getTime();
                        log.info(" 1111111111111111 retrieve data end! size="+ dataviewDataset.rows.size()+" take time="+ timeTaken2);
                        
                        if ( dataviewDataset.getRows().isEmpty() && retry<5 )
                        {
                            Tool.SleepAWhileInMS(200);
                        }
                        else
                            break;
                    }
                }
                else
                    break;
            }
                
            if ( processedItems > 0 )
                dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
            
            writer.close();
            
            return exportFilename;
        }
        catch(Exception e) 
        {            
            log.error(" exportDataToTxt() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                 
            throw e;
        }
        finally
        {
            if ( writer != null )
                writer.close();
        }          
    }
  
    public static String exportDataToExcel(DataService.Client dataService, DataExportTask exportDataTask, ExportJob exportJob, AnalyticDataview dataview, String tmpDirectory) throws Exception
    {
        String[] columns;
        Map<String,String> columnDataTypes;
        String column;

        int processedItems;
        String dataobjectTypeName;
        String tableColumnStr;
        Map<String,String> tableColumnNameMap = new HashMap<>();
        Date currentDate;
        long dateValue;
        String exportFilename;
        int dataType;
        String value;
        ByteBuffer buf;

        try
        {
            exportFilename = String.format("%s/%s.%s.%s",tmpDirectory,UUID.randomUUID().toString(),exportDataTask.getExportFileName(),FileType.findByValue(exportDataTask.getExportFileType()).getExt());

            FileOutputStream fos = new FileOutputStream(exportFilename);
           
            Workbook workbook = new XSSFWorkbook();
                                                 
            Sheet sheet = workbook.createSheet(exportDataTask.getExportFileTitle()); 
                                     
            columnDataTypes = dataService.getDataviewColumnTypes(dataview.getOrganizationId(),dataview.getId());
            
            dataobjectTypeName = columnDataTypes.get("dataobjectTypeName");
            log.info("333333333 dataobjectTypeName = "+dataobjectTypeName);
          
            boolean ifDataobjectTypeWithChangedColumnName = columnDataTypes.get("ifDataobjectTypeWithChangedColumnName").equals("true");
                           
            if ( dataview.getType() == DataviewType.NORMAL.getValue() )
                columns = dataview.getColumns().split("\\;");
            else
            {
                if ( dataview.getTimeAggregationColumn() == null || dataview.getTimeAggregationColumn().trim().isEmpty() )
                    columns = dataview.getAggregationColumns().split("\\;");
                else
                    columns = (dataview.getTimeAggregationColumn().trim()+";"+dataview.getAggregationColumns()).split("\\;");
            }
                           
            buf = ByteBuffer.wrap(Tool.serializeObject(dataview));
            Dataset dataviewDataset = dataService.getDataviewData(exportDataTask.getOrganizationId(), dataview.getId(),buf,null,CommonKeys.SIZE_FOR_DATAVIEW_BATCH,600, true);
            String scrollId = dataviewDataset.getScrollId();
         
            int j = 0;
            int k = 0;
            Row excelRow = sheet.createRow(k);
           
            for(String exportColumn : columns)
            {
                tableColumnStr = Tool.changeTableColumnName(dataobjectTypeName,exportColumn,ifDataobjectTypeWithChangedColumnName);               
                excelRow.createCell(j).setCellValue(tableColumnStr);  
                j++;
            }
         
            processedItems = 0;          
     
            while(true)
            {
                log.info("2222222222222222 get search result! number ="+dataviewDataset.getRows().size());
             
                for(Map<String,String> row : dataviewDataset.getRows())
                {
                    k++;
                    excelRow = sheet.createRow(k);
            
                    j=0;
                   
                    for(String exportColumn : columns)
                    {
                        value = row.get(exportColumn);

                        if ( value == null || value.equals("null") )
                            value = "";
                        else
                        {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);
                            
                            value = Tool.replaceComma(value);
                            
                            if ( !value.isEmpty() && Tool.isNumber(value)  )
                                value = Tool.normalizeNumber(value);
                        }

                        value = Tool.removeQuotation(value);
                        
                        if ( exportColumn.startsWith("count("))
                            column = "*";
                        else
                        if ( exportColumn.contains("(") )
                            column = exportColumn.substring(exportColumn.indexOf("(")+1,exportColumn.indexOf(")"));
                        else
                            column = exportColumn;

                        dataType = Integer.parseInt((String)columnDataTypes.get(column));

                        if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue()  )
                        {
                            if ( dataview.getType() == DataviewType.AGGREGATION.getValue() )
                            {
                                try 
                                {
                                    currentDate = Tool.changeFromGmt(new Date(Long.parseLong(value)));
                                    value = Tool.convertDateToTimestampString(currentDate);
                                }
                                catch(Exception e)
                                {
                                    log.info(" no need to convert");
                                    value = value.substring(0,19);
                                }
                            }
                            else
                            {                                             
                                dateValue = Tool.convertESDateStringToLong(value);
                                currentDate = Tool.changeFromGmt(new Date(dateValue));
                                
                                if ( dataType == MetadataDataType.DATE.getValue() )
                                    value = Tool.convertDateToDateString(currentDate);
                                else
                                    value = Tool.convertDateToTimestampStringWithMilliSecond1(currentDate);
                            }
                        }
 
                        excelRow.createCell(j).setCellValue(value);
                        j++;
                    }
 
                    processedItems++;
                                          
                    if ( processedItems >= CommonKeys.FILE_BATCH_COMMIT_NUMBER )
                    {
                        dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
                        processedItems = 0;
                    }
                }
            
                if ( dataviewDataset.hasMoreData )
                {
                    int retry = 0;
                    while(true)
                    {
                        retry++;
                        
                        Date date2 = new Date();
                        log.info(" 11111111111111 start retrieve data!");
                        
                        dataviewDataset = dataService.getRemainingDataviewData(exportDataTask.getOrganizationId(), dataview.getId(), buf, scrollId, 600,false);
                        
                        long timeTaken2 = new Date().getTime() - date2.getTime();
                        log.info(" 1111111111111111 retrieve data end! size="+ dataviewDataset.rows.size()+" take time="+ timeTaken2);
                        
                        if ( dataviewDataset.getRows().isEmpty() && retry<5 )
                        {
                            Tool.SleepAWhileInMS(200);
                        }
                        else
                            break;
                    }
                }
                else
                    break;
            }

            workbook.write(fos);
            fos.close();
            
            if ( processedItems > 0 )
                dataService.increaseDataExportJobItemNumber(exportDataTask.getOrganizationId(), exportJob.getJobId(), processedItems,"");
                
            return exportFilename;
        }
        catch(Exception e)
        {
            log.error(" exportDataToExcel() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }        
    }
  
  /*  public static String generateTxtExportDocument11(DataService.Client dataService, DataExportTask exportDataTask, AnalyticDataview dataview, AnalyticQuery query,String tmpDirectory) throws Exception 
    {
        int i;
        String[] documentLine;
        List<String[]> documentContents = new ArrayList<>();
        Map<String,Object> dataviewData;
        List<DataobjectVO> dataobjectVOList;
        String[] exportColumns;
        String exportFilename = "";
                
        try
        {           
            ByteBuffer buf = dataService.getDataviewResult(exportDataTask.getOrganizationId(), dataview.getId(), null,300*5,600);
            dataviewData = ( Map<String,Object>)Tool.deserializeObject(buf.array());
            
            String scrollId = (String)dataviewData.get("scrollId");

            if ( dataview.getType() == DataviewType.NORMAL.getValue() )
                exportColumns = dataview.getColumns().split("\\;");
            else
            {
                if ( dataview.getTimeAggregationColumn() == null || dataview.getTimeAggregationColumn().trim().isEmpty() )
                    exportColumns = dataview.getAggregationColumns().split("\\;");
                else
                    exportColumns = (dataview.getTimeAggregationColumn().trim()+";"+dataview.getAggregationColumns()).split("\\;");
            }
                
            if ( tmpDirectory.endsWith("/") || tmpDirectory.endsWith("\\") )
                exportFilename = String.format("%s%s.%s.%s",tmpDirectory,UUID.randomUUID().toString(),exportDataTask.getExportFileName(),FileType.findByValue(exportDataTask.getExportFileType()).getExt());
            else
                exportFilename = String.format("%s/%s.%s.%s",tmpDirectory,UUID.randomUUID().toString(),exportDataTask.getExportFileName(),FileType.findByValue(exportDataTask.getExportFileType()).getExt());

            File file = new File(exportFilename);
            if ( !file.exists() )
                file.createNewFile(); 
            
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write(exportDataTask.getExportFileTitle()+"\n\n");
            
            // field name
            StringBuilder line = new StringBuilder();
             
            for(String exportColumn : exportColumns)
                line.append(exportColumn).append(",");
            
            String fieldName = line.toString();
            fieldName = fieldName.substring(0, fieldName.length()-1);
            writer.write(fieldName+"\n");
            
            while(true)
            {
                dataobjectVOList = (List<DataobjectVO>)dataviewData.get("dataobjectVOs");      
                             
                for(DataobjectVO obj : dataobjectVOList)
                {
                    i=0;
                    documentLine = new String[exportColumns.length];
 
                }
                                     
                if ( !dataobjectVOList.isEmpty() )
                {
                    buf = dataService.getRemainingDataviewResult(exportDataTask.getOrganizationId(), Integer.parseInt(query.getTargetRepositories()), scrollId, 600);
                    dataviewData = ( Map<String,Object>)Tool.deserializeObject(buf.array());
                }
                else
                    break;
            }

        }
        catch(Exception e) {
            throw e;
        }
         
        return exportFilename;   
    }*/
     
    public static void generateEXCELDocument(String tmpFilename, String filetitle, List<String[]> documentContents, String[] columns) throws Exception 
    {
        Workbook workbook = null;
        
        try
        {            
           //Create the os for the xlsx/xls file
            FileOutputStream fos = new FileOutputStream(tmpFilename);

            if(tmpFilename.toLowerCase().endsWith("xlsx"))
                workbook = new XSSFWorkbook();
            else 
            if(tmpFilename.toLowerCase().endsWith("xls"))
                workbook = new HSSFWorkbook();
                                      
            Sheet sheet = workbook.createSheet(filetitle); 
                       
            Row row = sheet.createRow(0);

            for(int i=0;i<columns.length;i++)
            {
                String cName = columns[i];
                
                if ( cName.contains("-") )
                    cName = cName.substring(cName.indexOf("-")+1);
                
                row.createCell(i).setCellValue(cName);  
            }

            // line
            int k=0;
            
            for(String[] contentLine : documentContents)
            {
                k++;
                row = sheet.createRow(k);
                                
                for(int j=0;j<contentLine.length;j++)
                    row.createCell(j).setCellValue(contentLine[j]);
            }
 
            workbook.write(fos);
            fos.close();
        }
        catch(Exception e)
        {     
            log.error("generateEXCELDocument() failed! e="+e.getMessage());
            throw e;
        }
    }    
        
    public static void generatePDFDocument(String tmpFilename, String filetitle, List<String[]> documentContents, String[] columns) throws Exception 
    {       
        try
        {
            Document document = new Document(PageSize.A4, 50, 50, 50, 50);
            PdfWriter.getInstance(document, new FileOutputStream(tmpFilename));

            document.open();
   
            // title
            document.add(new Paragraph(filetitle,setChineseFont()));  
            document.add(new Paragraph(" "));

            // field name
            StringBuilder line = new StringBuilder();
             
            for(String exportColumn : columns)
                line.append(exportColumn).append(",");
            
            String fieldNames = line.toString();
            fieldNames = fieldNames.substring(0, fieldNames.length()-1);
                  
            document.add(new Paragraph(fieldNames,setChineseFont()));
            document.add(new Paragraph(" "));            

            for(String[] contentLine : documentContents)
            {               
                line = new StringBuilder();
                for(int i=0;i<contentLine.length;i++)
                {
                    line.append(contentLine[i]);
                    
                    if ( i == contentLine.length-1 )  // last one
                        continue;
                    
                    line.append(",");
                }
                document.add(new Paragraph(line.toString(),setChineseFont()));  
            }
            document.close();
        }
        catch(Exception e)
        {     
            log.error("generatePDFDocument() failed! e="+e.getMessage());
            throw e;
        }
    }

    public static void generateTXTDocument(String tmpFilename, String filetitle, List<String[]> documentContents, String[] columns,String encoding) throws Exception 
    {
        try
        {     
            //File file = new File(tmpFilename);
            //if ( !file.exists() )
            //    file.createNewFile(); 
            
            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFilename),encoding);
             
            //BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            //writer.write(filetitle+"\n\n");
            
            // field name
            StringBuilder line = new StringBuilder();
             
            for(String exportColumn : columns)
            {
                String cName = exportColumn;
                
                if ( cName.contains("-") )
                    cName = cName.substring(cName.indexOf("-")+1);
                
                line.append(cName).append(",");
            }
            
            String fieldName = line.toString();
            fieldName = fieldName.substring(0, fieldName.length()-1);
            writer.write(fieldName+"\n");
            
            for(String[] contentLine : documentContents)
            {               
                line = new StringBuilder();
                for(int i=0;i<contentLine.length;i++)
                {
                    if ( contentLine[i] == null || contentLine[i].equals("null") )
                        line.append("");
                    else
                        line.append(contentLine[i]);
                    
                    if ( i == contentLine.length-1 )  // last one
                        continue;
                    
                    line.append(",");
                }
                writer.write(line.toString()+"\n");  
            }
            
            writer.close();
        }
        catch(Exception e)
        {     
            log.error("generateTXTDocument() failed! e="+e.getMessage());
            throw e;
        }
    }
    
    private static Font setChineseFont() throws Exception 
    {
        BaseFont base = null;
        Font fontChinese = null;
        
        try {
            base = BaseFont.createFont("STSong-Light", "UniGB-UCS2-H",
            BaseFont.EMBEDDED);
            fontChinese = new Font(base, 12, Font.NORMAL);
        } catch ( Exception e) {
            throw e;
        }
        return fontChinese;
    }    
         
    public static String saveToDatabaseFromSqldb(long exportJobId,DataService.Client dataService,int organizationId,DatasourceConnection sourceDatasourceConnection,DatasourceConnection targetDatasourceConnection,String exportTableName,List<String> tableColumns,DataobjectType selectedDataobjectType, List<Map<String, Object>> metadataInfoList,int databaseUpdateMode,int pageSize,int repositoryId,int catalogId,String filterStr,String searchKeyword,boolean retrieveAllData,String lastProcessedItem,int comparingFieldType,String comparingField) throws SQLException 
    {
        String insertSql = null;
        String updateSql = null;
        String whereSql = null;
      
        String sourceSchema;
        String targetSchema;
      
        java.sql.Connection targetConn = null;
        java.sql.Statement targetStmt = null;
        java.sql.Connection sourceConn = null;

        DatabaseType sourceDatabaseType = null;
        DatabaseType targetDatabaseType = null;
        
        Map<String,String> columnLenPrecisionMap = null;
        List<String> primaryKeyNames = null;
        Map<String,String> tableColumnNameMap = new HashMap<>();
 
        int retry;
        String exportColumnStr;
        String tableColumnName;
        String value;
        String tableColumnStr;
        int size;
        int updateCount=0;
        int insertCount=0;
        int processed;
        String sql;
        String orderByStr = "";
        
        int count;
        PreparedStatement statement = null;
        ResultSet resultSet;    
             
        try
        {             
            targetDatabaseType = JDBCUtil.getDatabaseConnectionDatabaseType(targetDatasourceConnection);
                  
            targetSchema = JDBCUtil.getDatabaseConnectionSchema(targetDatasourceConnection);
            targetConn = JDBCUtil.getJdbcConnection(targetDatasourceConnection);
            
            primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(selectedDataobjectType.getMetadatas());
            columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(selectedDataobjectType.getMetadatas());
              
            boolean ifDataobjectTypeWithChangedColumnName = true;
                        
            for(String kName : primaryKeyNames)
            {
                boolean hasKeyColumnSelected = false;
                
                for(String tName : tableColumns )
                {
                    String c = tName.substring(0,tName.indexOf("-"));
                    
                    if ( c.equals(kName) )
                    {
                        hasKeyColumnSelected = true;
                        break;
                    }
                }
                
                if ( hasKeyColumnSelected == false)
                {
                    primaryKeyNames.clear();
                    break;
                }   
            }
            
            targetStmt = targetConn.createStatement(); 
            
            Map<String,String> properties = Util.getDatasourceConnectionProperties(targetDatasourceConnection.getProperties());
            String tableSpace=properties.get("table_space");
            String indexSpace=properties.get("index_space");
            
            if ( databaseUpdateMode == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue()  )
            {
                try
                {
                    if ( JDBCUtil.isTableExists(targetConn, targetSchema, exportTableName) )
                    {
                        sql = String.format("drop table %s",exportTableName);  log.info("11111 sql="+sql);
                        targetStmt.executeUpdate(sql);
                    }
                }
                catch(Exception ee)
                {
                    log.info(" drop table failed! e="+ee+" stacktrace="+ExceptionUtils.getStackTrace(ee));
                }
            }
           
            try
            {
                if ( JDBCUtil.isTableExists(targetConn, targetSchema, exportTableName) == false )
                {
                    List<String> sqlList = Util.createTableForDataobjectType(false,ifDataobjectTypeWithChangedColumnName,selectedDataobjectType.getName(),selectedDataobjectType.getDescription(),exportTableName,"",primaryKeyNames,targetDatabaseType,targetDatasourceConnection.getUserName(),metadataInfoList,tableColumns,columnLenPrecisionMap,tableSpace,indexSpace);

                    for(String createTableSql : sqlList)
                    {
                        log.info(" exceute create table sql ="+createTableSql);
                        targetStmt.executeUpdate(createTableSql);
                    }
                }
            }
            catch(Exception ee)
            {
                log.info(" create table failed e="+ee+" stacktrace="+ExceptionUtils.getStackTrace(ee));
            }
  
            targetConn.setAutoCommit(false);
            
            String columnNameStr = "";

            for(String exportColumn : tableColumns)
            {
                exportColumnStr = exportColumn.substring(0,exportColumn.indexOf("-"));
                tableColumnName = Tool.changeTableColumnName(selectedDataobjectType.getName(),exportColumnStr,ifDataobjectTypeWithChangedColumnName);
                columnNameStr += tableColumnName+",";
                
                tableColumnNameMap.put(exportColumnStr, tableColumnName);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length()-1);
         
            String sourceTableName = selectedDataobjectType.getName();
                    
            sourceConn = JDBCUtil.getJdbcConnection(sourceDatasourceConnection);
            log.info("sourceConn = "+sourceConn);
     
            sourceDatabaseType = JDBCUtil.getDatabaseConnectionDatabaseType(sourceDatasourceConnection);

            sourceSchema = Util.getDatabaseSchema(sourceDatasourceConnection);
                       
            //primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(selectedDataobjectType.getMetadatas());   
            
            String primaryKeyNameStr = "";
            
            for(String columName : primaryKeyNames)
            {
                tableColumnName = Tool.changeTableColumnName(selectedDataobjectType.getName(),columName,true);
                primaryKeyNameStr += tableColumnName +",";
            }
            
            primaryKeyNameStr = primaryKeyNameStr.substring(0, primaryKeyNameStr.length()-1);
                        
            String lastProcessedItemDatetimeStr;
            
            if ( filterStr == null || filterStr.trim().isEmpty() )
                filterStr = "";
            
            String filter = filterStr;          
            
            if ( comparingField == null || comparingField.trim().isEmpty() )
            {
                orderByStr = "";
            }
            else
            {  
                orderByStr = String.format(" order by %s ",comparingField);
                    
                if ( comparingFieldType == MetadataDataType.TIMESTAMP.getValue() )
                {                   
                    if ( lastProcessedItem == null || lastProcessedItem.isEmpty() )
                    {
                        lastProcessedItemDatetimeStr = Tool.convertDateToString(new Date(0),"yyyy-MM-dd HH:mm:ss");
                        
                        if ( filter.isEmpty() )
                            filter = String.format("where %s>'%s'",comparingField,lastProcessedItemDatetimeStr);
                        else
                            filter = String.format("where ( %s ) and %s>'%s'",filter,comparingField,lastProcessedItemDatetimeStr);
                    }
                    else
                    {
                        if ( filter.isEmpty() )
                            filter = String.format("where %s>'%s'",comparingField,lastProcessedItem); 
                        else
                            filter = String.format("where ( %s ) and %s>'%s'",filter,comparingField,lastProcessedItem); 
                    }
                }
                else
                {          
                    if ( lastProcessedItem == null || lastProcessedItem.isEmpty() )
                    {
                        if ( filter.isEmpty() )
                            filter = String.format("where %s>0",comparingField);
                        else
                            filter = String.format("where ( %s ) and %s>0",filter,comparingField);
                    }
                    else
                    {
                        if ( filter.isEmpty() )
                            filter = String.format("where %s>%s",comparingField,lastProcessedItem);
                        else
                            filter = String.format("where ( %s ) and %s>%s",filter,comparingField,lastProcessedItem);
                    }
                }          
            }           

            sql = String.format("select count(*) from %s%s %s",sourceSchema,sourceTableName,filter);
         
            log.info(" countQuerySQL ="+sql);
            
            statement = sourceConn.prepareStatement(sql);
            resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();
 
            count = resultSet.getInt(1);
                                 
            log.info(" count="+count);
 
            if ( count == 0 )
            {
                log.info("count is 0, no more result!");
                return "";
            }
            
            dataService.updateDataExportJobTotalItemNumber(organizationId, exportJobId, count);
                                
            processed = 0;            
       
            int PAGE_SIZE = 50000; //50000
            int page = 0;
            int i=0;
            String lastProcssedItem = "";
            
            while(true)
            {
                page++;

                long startLine = (page-1)*PAGE_SIZE + 1;
                 
                String querySQL = generateQuerySQL(sourceDatabaseType,startLine,PAGE_SIZE,sourceSchema,sourceTableName,primaryKeyNameStr,filter,orderByStr);
                
                log.info(String.format(" querySql = %s ",querySQL));

                statement = sourceConn.prepareStatement(querySQL);
                statement.setFetchSize(1000);

                resultSet = JDBCUtil.executeQuery(statement);

                if ( resultSet.next() == false )
                {
                    log.info(" no more result!");
                    
                    JDBCUtil.close(resultSet);
                    JDBCUtil.close(statement);
                
                    break;
                }
                else
                {
                    do
                    {
                        i++;
                        
                        if ( comparingField != null && !comparingField.trim().isEmpty() )
                            lastProcssedItem = resultSet.getString(comparingField);
                        
                        log.info(" processing line = "+i+", lastProcssedItem"+lastProcssedItem);
 
                        processed++;
                        
                        if ( databaseUpdateMode == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue() )
                        {
                            insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",exportTableName,columnNameStr);
                            updateSql = String.format("UPDATE %s SET ",exportTableName);
                            whereSql = String.format("WHERE ");
                        }
                        else
                        {
                            insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",exportTableName,columnNameStr);
                            updateSql = String.format("UPDATE %s SET ",exportTableName); 
                            whereSql = String.format("WHERE ");
                        }

                        for(String exportColumn : tableColumns)
                        {
                            exportColumnStr = exportColumn.substring(0,exportColumn.indexOf("-"));

                            tableColumnStr = tableColumnNameMap.get(exportColumnStr);

                            value =  resultSet.getString(tableColumnStr);

                            if ( value == null || value.equals("null") )
                                value = "";
                            else
                            {
                                value = value.trim();

                                value = Tool.removeSpecialChar(value);

                                if ( !value.isEmpty() && Tool.isNumber(value)  )
                                    value = Tool.normalizeNumber(value);
                            }

                            value = Tool.removeQuotation(value);

                            int dataType = Util.getMetadataDataTypeString(exportColumnStr,metadataInfoList);

                          /*  if ( dataType == MetadataDataType.DATE.getValue() )
                            {
                                value = resultSet.getDate(tableColumnStr) + "";
                            }
                            else
                            if ( dataType == MetadataDataType.TIMESTAMP.getValue()  )
                            {
                                value = resultSet.getDate(tableColumnStr) + " " + resultSet.getTime(tableColumnStr);
                            }*/

                            if ( dataType == MetadataDataType.TIMESTAMP.getValue() && (targetDatabaseType == DatabaseType.SQLSERVER || targetDatabaseType == DatabaseType.SQLSERVER2000) )
                            {
                                long dateValue = Tool.convertESDateStringToLong(value);
                                value = Tool.convertDateToString(new Date(dateValue),"yyyy-MM-dd HH:mm:ss");
                            }

                            if ( targetDatabaseType == DatabaseType.ORACLE || targetDatabaseType == DatabaseType.ORACLE_SERVICE )
                            {
                                if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                     dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                     dataType == MetadataDataType.DOUBLE.getValue() )
                                {
                                    if( value.isEmpty() )
                                    {
                                        String clp = columnLenPrecisionMap.get(exportColumnStr);
                                        String defaultNullVal = Tool.getDefaultNullVal(clp);

                                        insertSql += String.format("'%s',",defaultNullVal);
                                    }
                                    else
                                        insertSql += String.format("%s,",value);
                                }
                                else
                                if ( dataType == MetadataDataType.DATE.getValue() )
                                    insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd'),",value);
                                else
                                if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                                {
                                    if ( value.length()>18 )
                                        value = value.substring(0, 19);
                                                                            
                                    insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value);
                                }
                                else
                                    insertSql += String.format("'%s',",value);      
                            }
                            else
                            {
                                if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                     dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                     dataType == MetadataDataType.DOUBLE.getValue() )
                                {
                                    if( value.isEmpty() )
                                    {
                                        String clp = columnLenPrecisionMap.get(exportColumnStr);
                                        String defaultNullVal = Tool.getDefaultNullVal(clp);

                                        insertSql += String.format("'%s',",defaultNullVal);
                                    }
                                    else
                                        insertSql += String.format("%s,",value);
                                }
                                else
                                     insertSql += String.format("'%s',",value);                        
                            }

                            // for updatesql
                            boolean isPrimaryKey = false;

                            for(String kName : primaryKeyNames)
                            {
                                if( kName.equals(exportColumnStr) )
                                {
                                    isPrimaryKey = true;
                                    break;
                                }
                            }
                            
                            if ( targetDatabaseType == DatabaseType.ORACLE || targetDatabaseType == DatabaseType.ORACLE_SERVICE )
                            {
                                if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                    dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                    dataType == MetadataDataType.DOUBLE.getValue() )
                                {
                                    if( value.isEmpty() )
                                    {
                                        String clp = columnLenPrecisionMap.get(exportColumnStr);
                                        String defaultNullVal = Tool.getDefaultNullVal(clp);

                                        if ( isPrimaryKey )
                                            whereSql += String.format(" %s = '%s' AND",tableColumnStr,defaultNullVal);
                                        else
                                            updateSql += String.format(" %s = '%s',",tableColumnStr,defaultNullVal);
                                    }
                                    else
                                    {
                                        if ( isPrimaryKey )
                                            whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                        else
                                            updateSql += String.format(" %s = %s,",tableColumnStr,value);
                                    }
                                }
                                else
                                if ( dataType == MetadataDataType.DATE.getValue() )
                                {
                                    value = String.format("TO_DATE('%s', 'yyyy-mm-dd')",value);
                                    
                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                    else
                                        updateSql += String.format(" %s = %s,",tableColumnStr,value);
                                }
                                else
                                if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                                {
                                    if ( value.length()>18 )
                                        value = value.substring(0, 19);
                                          
                                    value = String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss')",value);
                                    
                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                    else
                                        updateSql += String.format(" %s = %s,",tableColumnStr,value);
                                }
                                else
                                {
                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = '%s' AND",tableColumnStr,value);
                                    else
                                        updateSql += String.format(" %s = '%s',",tableColumnStr,value);
                                }                                
                            }
                            else
                            {                                 
                                if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                    dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                    dataType == MetadataDataType.DOUBLE.getValue() )
                                {
                                    if( value.isEmpty() )
                                    {
                                        String clp = columnLenPrecisionMap.get(exportColumnStr);
                                        String defaultNullVal = Tool.getDefaultNullVal(clp);

                                        if ( isPrimaryKey )
                                            whereSql += String.format(" %s = '%s' AND",tableColumnStr,defaultNullVal);
                                        else
                                            updateSql += String.format(" %s = '%s',",tableColumnStr,defaultNullVal);
                                    }
                                    else
                                    {
                                        if ( isPrimaryKey )
                                            whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                        else
                                            updateSql += String.format(" %s = %s,",tableColumnStr,value);
                                    }
                                }
                                else
                                {
                                    if ( isPrimaryKey )
                                        whereSql += String.format(" %s = '%s' AND",tableColumnStr,value);
                                    else
                                        updateSql += String.format(" %s = '%s',",tableColumnStr,value);
                                }                                
                            }
                        }

                        insertSql = insertSql.substring(0,insertSql.length()-1)+")";

                        if ( updateSql != null )
                        {
                            updateSql = updateSql.substring(0,updateSql.length()-1);

                            if ( whereSql != null && !whereSql.trim().isEmpty() )
                                whereSql = whereSql.substring(0,whereSql.length()-3);
                            else
                                whereSql = "";

                            updateSql += " "+ whereSql;
                        }

                        retry = 0;

                        while(true)
                        {
                            retry++;

                            try
                            {
                                targetStmt.executeUpdate(insertSql);
                                //log.info(" insert succeed! dataobjectId="+obj.getId());
                                insertCount++;
                                //log.info(" insertCount="+insertCount);
                                break;
                            }
                            catch(Exception e)
                            {
                                //log.info(" insert into database failed! try update! e="+e+" sql="+insertSql);

                                try{
                                    targetConn.commit();
                                }
                                catch(Exception ee){
                                }

                                try
                                {
                                    int ret= targetStmt.executeUpdate(updateSql);
                                    //log.info(" update succeed! dataobjectId="+obj.getId());

                                    if( ret==1 )
                                    {
                                        updateCount++;
                                        log.info(" update succeed! updateCount="+updateCount+" updateSql="+updateSql);
                                    }

                                    break;
                                }
                                catch(Exception ee)
                                {
                                    Tool.SleepAWhileInMS(100);

                                    try{
                                        targetConn.commit();
                                    }
                                    catch(Exception eee){
                                    }

                                    if ( retry>3 )
                                    {
                                        log.error(" process data failed! e="+ee+" updatesql="+updateSql+" insertSql="+insertSql);
                                        throw e;
                                    }
                                }
                            }
                        }

                        if ( i % CommonKeys.JDBC_BATCH_COMMIT_NUMBER == 0 )
                        {
                            targetConn.commit();

                            log.info("222222222222222222222 commit to database per "+CommonKeys.JDBC_BATCH_COMMIT_NUMBER);
                            Tool.SleepAWhileInMS(1000);
                        }
                    }
                    while(resultSet.next());   
                }

                targetConn.commit();   
               
                // get remaining data
                
                while(true)
                {
                    try
                    {
                        if ( exportJobId > 0 )
                        {
                            dataService.increaseDataExportJobItemNumber(organizationId, exportJobId, processed,lastProcssedItem);
                            processed = 0;
                        }
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error("dataService.updateClientJobStatus() failed! e="+e);
                        Tool.SleepAWhile(1, 0);
                    }
                }
                       
                JDBCUtil.close(resultSet);
                JDBCUtil.close(statement);
            }
          
            return "";
            //return addMessage(FacesMessage.SEVERITY_INFO,"operation.info.0001",datasourceConnection.getName(),tableName);
        }
        catch(Throwable e) 
        {
            String errorInfo = "77777777777777 processing failed!  e="+e+" sql="+insertSql+ " stacktrace = "+ExceptionUtils.getStackTrace(e);
            log.info(errorInfo);
            
            if ( targetConn != null )
                targetConn.rollback();
            
            if ( databaseUpdateMode == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue() )
            {
                try 
                {
                    sql = String.format("drop table %s",exportTableName);
                    targetStmt.executeUpdate(sql);
                }
                catch(Exception ee)
                {
                    log.error(" drop temp table failed! table="+exportTableName+" ee="+ee);
                }
            }
 
            return e.getMessage();
        }        
        finally
        {
            JDBCUtil.close(targetConn);           
            JDBCUtil.close(sourceConn);
        }  
    }
          
    private static String generateQuerySQL(DatabaseType databaseType,long startLine,int pageSize,String schema,String tableName,String primaryKeyNameStr,String filter,String orderByStr) // 1,1000; 1001,1000
    {
        String querySql;

        if ( filter == null || filter.trim().isEmpty() )
        {
            if ( schema.isEmpty() )
                querySql = String.format("select * from %s %s",tableName,orderByStr);
            else
                querySql = String.format("select * from %s%s %s",schema,tableName,orderByStr);
        }
        else
        {
            if ( schema.isEmpty() )
                querySql = String.format("select * from %s %s %s",tableName,filter,orderByStr);
            else
                querySql = String.format("select * from %s%s %s %s",schema,tableName,filter,orderByStr);
        }

        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            querySql = String.format("SELECT * FROM （ SELECT A.*, ROWNUM RN FROM ( %s ) A WHERE ROWNUM <= %d )WHERE RN >= %d",querySql,startLine+pageSize-1, startLine);
        }
        else
        if ( databaseType == DatabaseType.MYSQL )
        {
            querySql = String.format("%s limit %d,%d",querySql,startLine-1,pageSize);
        }
        else
        if ( databaseType == DatabaseType.GREENPLUM )
        {
            querySql = String.format("%s offset %d limit %d",querySql,startLine-1,pageSize);
        }
              
        return querySql;
    }
          
    public static String saveToDatabaseFromES(long exportJobId,DataService.Client dataService,int organizationId,DatasourceConnection targetDatabase,String tableName,List<String> tableColumns,DataobjectType selectedDataobjectType, List<Map<String, Object>> metadataInfoList,int databaseUpdateMode,int pageSize,int repositoryId,int catalogId,String filterStr,String searchKeyword,boolean retrieveAllData) throws SQLException 
    {
        String insertSql = null;
        String updateSql = null;
        String whereSql = null;
      
        String schema;
        String driverName;
        java.sql.Connection conn = null;
        java.sql.Statement stmt = null;
        String column;        
        DatabaseType databaseType = null;
        Map<String,String> columnLenPrecisionMap = null;
        List<String> primaryKeyNames = null;
        Map<String,String> tableColumnNameMap = new HashMap<>();
 
        SearchRequest request = null;
        SearchResponse response = null;
        List<Map<String,String>> searchResults = null;
        String scrollId = null;
        int retry;
        JSONObject resultFields;
        String exportColumnStr;
        String tableColumnName;
        String value;
        String tableColumnStr;
        int size;
        int updateCount=0;
        int insertCount=0;
        int processed;
        int searchScope;
        String sql;
                
        try
        {             
            searchScope = SearchScope.ALL_DATA_IN_CATALOG.getValue();
                          
            databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(targetDatabase);
                  
            schema = JDBCUtil.getDatabaseConnectionSchema(targetDatabase);
            conn = JDBCUtil.getJdbcConnection(targetDatabase);
            
            primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(selectedDataobjectType.getMetadatas());
            columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(selectedDataobjectType.getMetadatas());
              
            boolean ifDataobjectTypeWithChangedColumnName = true;
                        
            for(String kName : primaryKeyNames)
            {
                boolean hasKeyColumnSelected = false;
                
                for(String tName : tableColumns )
                {
                    String c = tName.substring(0,tName.indexOf("-"));
                    
                    if ( c.equals(kName) )
                    {
                        hasKeyColumnSelected = true;
                        break;
                    }
                }
                
                if ( hasKeyColumnSelected == false)
                {
                    primaryKeyNames.clear();
                    break;
                }   
            }
            
            stmt = conn.createStatement(); 
            
            Map<String,String> properties = Util.getDatasourceConnectionProperties(targetDatabase.getProperties());
            String tableSpace=properties.get("table_space");
            String indexSpace=properties.get("index_space");
            
            if ( databaseUpdateMode == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue()  )
            {
                try
                {
                    if ( JDBCUtil.isTableExists(conn, schema, tableName) )
                    {
                        sql = String.format("drop table %s",tableName);  log.info("11111 sql="+sql);
                        stmt.executeUpdate(sql);
                    }
                }
                catch(Exception ee)
                {
                    log.info(" drop table failed! e="+ee+" stacktrace="+ExceptionUtils.getStackTrace(ee));
                }
            }
           
            try
            {
                if ( JDBCUtil.isTableExists(conn, schema, tableName) == false )
                {
                    List<String> sqlList = Util.createTableForDataobjectType(false,ifDataobjectTypeWithChangedColumnName,selectedDataobjectType.getName(),selectedDataobjectType.getDescription(),tableName,"",primaryKeyNames,databaseType,targetDatabase.getUserName(),metadataInfoList,tableColumns,columnLenPrecisionMap,tableSpace,indexSpace);

                    for(String createTableSql : sqlList)
                    {
                        log.info(" exceute create table sql ="+createTableSql);
                        stmt.executeUpdate(createTableSql);
                    }
                }
            }
            catch(Exception ee)
            {
                log.info(" create table failed e="+ee+" stacktrace="+ExceptionUtils.getStackTrace(ee));
            }
                                 
            int i=0;
            conn.setAutoCommit(false);
            
            String columnNameStr = "";

            for(String exportColumn : tableColumns)
            {
                exportColumnStr = exportColumn.substring(0,exportColumn.indexOf("-"));
                tableColumnName = Tool.changeTableColumnName(selectedDataobjectType.getName(),exportColumnStr,ifDataobjectTypeWithChangedColumnName);
                columnNameStr += tableColumnName+",";
                
                tableColumnNameMap.put(exportColumnStr, tableColumnName);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length()-1);
                
            processed = 0;            
              
            request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_GET_ALL_RESULTS.getValue());

            request.setSearchScope(searchScope);
            //request.setTargetIndexKeys(catalogPartitionMetadataValues);

            List<String> indexTypes = new ArrayList<>();
            indexTypes.add(selectedDataobjectType.getIndexTypeName());
            
            request.setTargetIndexTypes(indexTypes);
            request.setQueryStr(searchKeyword);
            request.setFilterStr(filterStr);
            request.setSearchFrom(0);
 
            request.setSortColumns("last_stored_time,ASC");

            //request.setColumns(columnsStr);

            retry = 0;

            while(true)
            {
                retry++;

                // get first dataobjectVO
                response = dataService.getDataobjectsWithScroll(request,pageSize,600);
                searchResults = response.getSearchResults();
                scrollId = response.getScrollId();
                //dataobjectVOs = Util.generateDataobjectsForDataset(searchresults,false);
        
                if ( !searchResults.isEmpty() )
                {
                    if ( exportJobId > 0 )
                    {
                        dataService.updateDataExportJobTotalItemNumber(organizationId, exportJobId, (int)response.getTotalHits());
                    }
                           
                    break;
                }

                if ( retry > 5 )
                {
                    log.error("dataService.getDataobjectsWithScroll() get 0 result! exit! ");
                    break;
                }

                log.info(" dataService.getDataobjectsWithScroll() get 0 result! retry="+retry);
                Tool.SleepAWhile(1, 0);
            }
                      
            while(true)
            {
                // process dataobjectVOs
                log.info(" 11111111111111111111 dataobjectVOs size="+searchResults.size());
                
                //for(DataobjectVO obj : dataobjectVOs)
                processed = 0;
                
                for(Map<String,String> searchResult : searchResults)
                {    
                    i++;
                    log.info(" processing line = "+i);
                    
                    resultFields = new JSONObject(searchResult.get("fields"));
                         
                    processed++;
                    
                    if ( databaseUpdateMode == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue() )
                    {
                        insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",tableName,columnNameStr);
                        updateSql = String.format("UPDATE %s SET ",tableName); 
                        whereSql = String.format("WHERE ");
                    }
                    else
                    {
                        insertSql = String.format("INSERT INTO %s ( %s ) VALUES (",tableName,columnNameStr);
                        updateSql = String.format("UPDATE %s SET ",tableName); 
                        whereSql = String.format("WHERE ");
                    }

                    for(String exportColumn : tableColumns)
                    {
                        exportColumnStr = exportColumn.substring(0,exportColumn.indexOf("-"));
                        //String value = obj.getThisDataobjectTypeMetadatas().get(exportColumnStr);
                        value =  resultFields.optString(exportColumnStr);
                        
                        tableColumnStr = tableColumnNameMap.get(exportColumnStr);

                        if ( value == null || value.equals("null") )
                            value = "";
                        else
                        {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);

                            if ( !value.isEmpty() && Tool.isNumber(value)  )
                                value = Tool.normalizeNumber(value);
                        }

                        value = Tool.removeQuotation(value);

                        int dataType = Util.getMetadataDataTypeString(exportColumnStr,metadataInfoList);

                        if ( dataType == MetadataDataType.DATE.getValue() )
                        {
                            long dateValue = Tool.convertESDateStringToLong(value);
                            Date gmtDate = Tool.changeFromGmt(new Date(dateValue));
                            value = Tool.convertDateToDateString(gmtDate);
                        }
                        else
                        if ( dataType == MetadataDataType.TIMESTAMP.getValue()  )
                        {
                            long dateValue = Tool.convertESDateStringToLong(value);
                            Date gmtDate = Tool.changeFromGmt(new Date(dateValue));
                            value = Tool.convertDateToTimestampStringWithMilliSecond1(gmtDate);
                        }
                        
                        if ( dataType == MetadataDataType.TIMESTAMP.getValue() && (databaseType == DatabaseType.SQLSERVER || databaseType == DatabaseType.SQLSERVER2000) )
                        {
                            long dateValue = Tool.convertESDateStringToLong(value);
                            Date gmtDate = Tool.changeFromGmt(new Date(dateValue));
                            value = Tool.convertDateToString(gmtDate,"yyyy-MM-dd HH:mm:ss");
                        }

                        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                 dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                 dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if( value.isEmpty() )
                                {
                                    String clp = columnLenPrecisionMap.get(exportColumnStr);
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    insertSql += String.format("'%s',",defaultNullVal);
                                }
                                else
                                    insertSql += String.format("%s,",value);
                            }
                            else
                            if ( dataType == MetadataDataType.DATE.getValue() )
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd'),",value);
                            else
                            if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),",value.substring(0, 19));
                            else
                                 insertSql += String.format("'%s',",value);      
                        }
                        else
                        {
                            if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                                 dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                                 dataType == MetadataDataType.DOUBLE.getValue() )
                            {
                                if( value.isEmpty() )
                                {
                                    String clp = columnLenPrecisionMap.get(exportColumnStr);
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    insertSql += String.format("'%s',",defaultNullVal);
                                }
                                else
                                    insertSql += String.format("%s,",value);
                            }
                            else
                                 insertSql += String.format("'%s',",value);                        
                        }

                        // for updatesql
                        boolean isPrimaryKey = false;

                        for(String kName : primaryKeyNames)
                        {
                            if( kName.equals(exportColumnStr) )
                            {
                                isPrimaryKey = true;
                                break;
                            }
                        }

                        if ( dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue() ||
                            dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue() ||
                            dataType == MetadataDataType.DOUBLE.getValue() )
                        {
                            if( value.isEmpty() )
                            {
                                String clp = columnLenPrecisionMap.get(exportColumnStr);
                                String defaultNullVal = Tool.getDefaultNullVal(clp);

                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = '%s' AND",tableColumnStr,defaultNullVal);
                                else
                                    updateSql += String.format(" %s = '%s',",tableColumnStr,defaultNullVal);
                            }
                            else
                            {
                                if ( isPrimaryKey )
                                    whereSql += String.format(" %s = %s AND",tableColumnStr,value);
                                else
                                    updateSql += String.format(" %s = %s,",tableColumnStr,value);
                            }
                        }
                        else
                        {
                            if ( isPrimaryKey )
                                whereSql += String.format(" %s = '%s' AND",tableColumnStr,value);
                            else
                                updateSql += String.format(" %s = '%s',",tableColumnStr,value);
                        }

                    }

                    insertSql = insertSql.substring(0,insertSql.length()-1)+")";

                    if ( updateSql != null )
                    {
                        updateSql = updateSql.substring(0,updateSql.length()-1);

                        if ( whereSql != null && !whereSql.trim().isEmpty() )
                            whereSql = whereSql.substring(0,whereSql.length()-3);
                        else
                            whereSql = "";

                        updateSql += " "+ whereSql;
                    }

                    retry = 0;

                    while(true)
                    {
                        retry++;

                        try
                        {
                            stmt.executeUpdate(insertSql);
                            //log.info(" insert succeed! dataobjectId="+obj.getId());
                            insertCount++;
                            //log.info(" insertCount="+insertCount);
                            break;
                        }
                        catch(Exception e)
                        {
                            log.info(" insert into database failed! try update! e="+e+" sql="+insertSql);

                            try{
                                conn.commit();
                            }
                            catch(Exception ee){
                            }
                                   
                            try
                            {
                                int ret= stmt.executeUpdate(updateSql);
                                //log.info(" update succeed! dataobjectId="+obj.getId());
                                
                                if( ret==1 )
                                {
                                    updateCount++;
                                    log.info(" update succeed! updateCount="+updateCount+" updateSql="+updateSql);
                                }
                                
                                break;
                            }
                            catch(Exception ee)
                            {
                                Tool.SleepAWhileInMS(100);

                                try{
                                    conn.commit();
                                }
                                catch(Exception eee){
                                }
                                
                                if ( retry>3 )
                                {
                                    log.error(" process data failed! e="+ee+" updatesql="+updateSql+" insertSql="+insertSql);
                                    throw e;
                                }
                            }
                        }
                    }

                    //i++;

                    if ( i % CommonKeys.JDBC_BATCH_COMMIT_NUMBER == 0 )
                    {
                        conn.commit();

                        log.info("222222222222222222222 commit to database per "+CommonKeys.JDBC_BATCH_COMMIT_NUMBER);
                        Tool.SleepAWhileInMS(1000);
                    }
                }

                conn.commit();   
                
                if ( retrieveAllData == false )
                    break;
                
                // get remaining data
                
                while(true)
                {
                    try
                    {
                        if ( exportJobId > 0 )
                        {
                            dataService.increaseDataExportJobItemNumber(organizationId, exportJobId, processed,"");
                        }
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error("dataService.updateClientJobStatus() failed! e="+e);
                        Tool.SleepAWhile(1, 0);
                    }
                }
                
                if ( response.hasMoreData )
                {
                    retry = 0;
                    
                    while(true)
                    {
                        retry++;
                        
                        response = dataService.getRemainingDataobjects(request,scrollId,600);

                        log.info("1111111111111111 get search result! number ="+response.getSearchResults().size()+" hasMoreData="+response.hasMoreData);
                        
                        if ( retry > 10 )
                        {
                            log.error("222222222222222222 failed to get remaining dataobject! retry="+retry);
                            break;
                        }
                        
                        if ( response.getSearchResults().isEmpty() )
                        {
                            Tool.SleepAWhileInMS(100);
                            continue;
                        }
                         
                        break;
                    }
 
                    searchResults = response.getSearchResults();
                    //dataobjectVOs = Util.generateDataobjectsForDataset(searchresults,false);
                }
                else
                    break;
            }
          
            return "";
            //return addMessage(FacesMessage.SEVERITY_INFO,"operation.info.0001",datasourceConnection.getName(),tableName);
        }
        catch(Throwable e) 
        {
            log.error("77777777777777 processing failed!  e="+e+" sql="+insertSql+ " stacktrace = "+org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e));
            
            if ( conn != null )
                conn.rollback();
            
            if ( databaseUpdateMode == DatabaseUpdateMode.DROP_AND_CREATE_NEW_TABLE.getValue() )
            {
                try 
                {
                    sql = String.format("drop table %s",tableName);
                    stmt.executeUpdate(sql);
                }
                catch(Exception ee)
                {
                    log.error(" drop temp table failed! table="+tableName+" ee="+ee);
                }
            }
 
            return e.getMessage();
        }        
        finally
        {
            JDBCUtil.close(stmt);
            JDBCUtil.close(conn);
        }  
    }
}
   
   
    