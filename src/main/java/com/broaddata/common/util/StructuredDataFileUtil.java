/*
 * StructureDataFileUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.Iterator;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.apache.thrift.TException;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.broaddata.common.model.enumeration.ColumnSeparatorType;
import com.broaddata.common.model.enumeration.DataValidationBehaviorType;
import com.broaddata.common.model.enumeration.EncodingType;
import com.broaddata.common.model.enumeration.JobItemProcessingStatisticType;
import com.broaddata.common.model.enumeration.JobTimeType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.enumeration.ValueRangeCheckType;
import com.broaddata.common.model.platform.StructuredFileDefinition;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DataServiceException;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import java.util.Scanner;
import org.apache.poi.ss.usermodel.DataFormatter;

public class StructuredDataFileUtil 
{
    static final Logger log = Logger.getLogger("StructuredDataFileUtil");
    
    public static Map<String,Object> importTxtFileToSystem(String filename,boolean onlyDoValidation,int organizationId,long jobId,int dataValidationBehaviorTypeId,InputStream in,int definitionProcessStartRow, int jobProcessStartRow, boolean processMaxData,int processRowNumber,DataService.Client dataService,CommonService.Client commonService,StructuredFileDefinition fileDefinition,int datasourceType, int datasourceId, int sourceApplicationId,int targetRepositoryType,int targetRepositoryId,boolean removeAroundQuotation,ClassWithCallBack callback,DataProcessingExtensionBase dataProcessingExtension,int alreadyProcessedLine,boolean needToCheckRecordDuplication,Counter serviceCounter)
    {
        int bufferSize = 50 * 1024 * 1024;//设读取文件的缓存为50MB
        int i=0;
        int processSucceedCount = 0;
        int unSentCount = 0;
        int processedLine = 0;
        int skipedLine = 0;
        int failedLine = 0;
        int duplicatedLine = 0;
        String line = "";
        int status = 0;
        
        Map<String,Object> importResults = new HashMap<>();
        Map<String,Object> processingResult;
        List<Map<String,Object>> processingResults= new ArrayList();

        List<Map<String,Object>> columnDefinitions;
        Map<String,Object> lineValidationResult;
        String lineValidationInfo;
        String lineProcessingInfo;
        Map<Integer,String> columnDefaultValues;
        StringBuilder errorInfo = new StringBuilder();
        Map<String,String> primaryKeyMap = new HashMap<>();
        Map<String,String> lineMap = new HashMap<>();
        String lineHash;
        String[] columns;
        String val;
        Date startTime;
        long timeTaken;
        String primaryKeyValue;
        Scanner sc = null;
        Date startTime1 = new Date();
        List<DataobjectInfo> dataobjectInfoList = new ArrayList<>();
           
        try
        {
            columnDefinitions = StructuredDataFileUtil.getColumnDefinitionsFromXmlStr(fileDefinition.getColumns());
            List<Integer> primaryKeyFieldIds = Util.getPrimaryKeyFieldIds(fileDefinition.getColumns());
   
            Map<String,Object> context = new HashMap<>();
            context.put("columnDefinitions", columnDefinitions);
            
            if ( dataProcessingExtension != null )
                dataProcessingExtension.setContext(context);

            String encoding = EncodingType.findByValue(fileDefinition.getEncodingType()).getEncodingStr();
            
            //encoding = "GB2312";
            log.info("file definition name="+fileDefinition.getName()+" encoding ="+encoding);
                        
            //BufferedReader reader = new BufferedReader(new InputStreamReader(in,encoding),bufferSize);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in,encoding));
        
            if ( jobId > 0 )
                dataService.setDatasourceEndJobTime(organizationId, jobId,JobTimeType.START_TIME.getValue());
            
            int k=0;
             
            /*sc = new Scanner(in, encoding);
            while( sc.hasNextLine() )
            {
                line = sc.nextLine();*/
                
            log.info("need to skip line:"+alreadyProcessedLine);
             
            while ((line = reader.readLine()) != null)
            {
                i++;
   
                if ( callback != null )
                    callback.sendBackData(i);

                if ( i < definitionProcessStartRow )
                    continue;
  
                if ( jobProcessStartRow > 0 && i < jobProcessStartRow )
                    continue;
                
                k++; 
                
                lineHash = DataIdentifier.generateHash(line);
                
                if ( line.trim().isEmpty() )
                    continue;
                
                columns = preProcessLine(line,removeAroundQuotation,ColumnSeparatorType.findByValue(fileDefinition.getColumnSeparatorType()).getSeparator(),ColumnSeparatorType.findByValue(fileDefinition.getColumnSeparatorType()).getReplaceSeparator());
                
               // if ( columns.length > 0)
               //     continue;
                
                if ( k <= alreadyProcessedLine )
                {
                    //lineHash = DataIdentifier.generateHash(line);
                    if ( needToCheckRecordDuplication )
                    {
                        lineMap.put(lineHash, String.valueOf(i));

                        primaryKeyValue = Util.getPrimaryKeyValue(primaryKeyFieldIds,fileDefinition,columns,lineHash);
                        primaryKeyMap.put(primaryKeyValue, String.valueOf(i));
                    }          
                      
                    if ( k % (CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM*200) == 0 )
                    {
                        log.info("line="+i+" already processed. skip!"+" alreadyProcessedLine="+alreadyProcessedLine);
                        dataService.setDatasourceEndJobTime(organizationId, jobId, JobTimeType.LAST_UPDATED_TIME.getValue());
                    }
                     
                    continue;
                }                
           
                if ( needToCheckRecordDuplication )
                {
                    val = lineMap.get(lineHash);
                    if ( val != null )
                    {
                        processedLine++;
                        log.warn(String.format("Duplicated line:  %d is duplicated with line %s!!!! line=(%s) filename=%s",i,val,line,filename));
                        duplicatedLine++;

                        if ( duplicatedLine >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                        {
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);
                            duplicatedLine = 0;
                        }

                        continue;
                    }
                                  
                    lineMap.put(lineHash, String.valueOf(i));
                }
  
                lineValidationResult = validateLine(columns,fileDefinition,columnDefinitions);
                
                lineValidationInfo = (String)lineValidationResult.get("lineValidationInfo");
                columnDefaultValues = (Map<Integer,String>)lineValidationResult.get("columnDefaultValues");
                
                if ( !lineValidationInfo.isEmpty() ) // invalid
                {
                    processingResult = new HashMap<>();
                    processingResult.put("index", i);
                    processingResult.put("line", line);
                    processingResult.put("lineProcessingInfo", lineValidationInfo);
                    processingResults.add(processingResult);
                    
                    errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,line,lineValidationInfo));
                    log.info("66666666666 invalid ="+errorInfo);
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue() )
                    {
                        failedLine++;
                        
                        if ( jobId > 0 )
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);
                                                
                        break;
                    }
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SKIP_RECORD_WHEN_INVALID.getValue() )
                    {
                        skipedLine++;
                                                
                        if ( jobId > 0 )
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), 1);
                                                
                        continue;
                    }
                }         
                
                primaryKeyValue = Util.getPrimaryKeyValue(primaryKeyFieldIds,fileDefinition,columns,lineHash);
                    
                if ( needToCheckRecordDuplication )
                {
                    String keyVal = primaryKeyMap.get(primaryKeyValue);
                    if ( keyVal != null )
                    {
                      /*  if ( !primaryKeyFieldIds.isEmpty()  )  // 有主键表
                        {
                            log.warn("Duplicated parimaryKey! key="+primaryKeyValue+" duplicated with line="+keyVal+" current line index="+i+" add linehash as key!");
                            continue;
                        }
                        else // 无主键表
                        {*/
                            processedLine++;
                            log.warn(String.format("Duplicated line:  %d is duplicated with line %s!!!! line=(%s) filename=%s",i,keyVal,line,filename));
                            duplicatedLine++;

                            if ( duplicatedLine >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                            {
                                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);
                                duplicatedLine = 0;
                            }

                            continue;
                        //}
                    }

                    primaryKeyMap.put(primaryKeyValue, String.valueOf(i));
                }
                
                if ( onlyDoValidation )
                {
                    processedLine++;
                    continue;
                }               
         
                lineProcessingInfo = processLine(dataobjectInfoList,i,primaryKeyValue,columns,columnDefaultValues,fileDefinition,removeAroundQuotation,primaryKeyFieldIds,sourceApplicationId,datasourceType,datasourceId,targetRepositoryId, columnDefinitions, dataService, ColumnSeparatorType.findByValue(fileDefinition.getColumnSeparatorType()).getSeparator(),dataProcessingExtension,jobId);    
                                
                if ( !lineProcessingInfo.isEmpty() )
                {
                    processingResult = new HashMap<>();
                    processingResult.put("index", i);
                    processingResult.put("line", line+"                  ");
                    processingResult.put("lineProcessingInfo", lineProcessingInfo);
                    
                    processingResults.add(processingResult);

                    errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,line,lineProcessingInfo));
                      
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue() )
                    {
                        failedLine++;                   
                        break;
                    }
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SKIP_RECORD_WHEN_INVALID.getValue() )
                    {
                        skipedLine++;                      
                        continue;
                    }
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SET_DEFAULT_VALUET_TO_INVALID_COLUMN.getValue() )
                    {
                        failedLine++;
                        
                        if ( jobId > 0 )
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);
                        
                        continue;
                    }
                }
                
                processedLine++;
                processSucceedCount++;
                unSentCount++;
                
                if ( unSentCount >= CommonKeys.DATASOURCE_END_TASK_DATAOBJECT_BATCH_NUM || 
                        jobId > 0 && processSucceedCount+duplicatedLine+skipedLine >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM  )
                {
                    startTime = new Date();
                    lineProcessingInfo = "";
                    
                    try
                    {
                        dataService.storeDataobjects(organizationId,targetRepositoryType,targetRepositoryId,dataobjectInfoList,jobId,true);
                                 
                        if ( serviceCounter != null )
                        {
                            serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_COUNT, unSentCount);
                            timeTaken = new Date().getTime()-startTime1.getTime();
                            serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_TIME_TAKEN, timeTaken); 

                            startTime1 = new Date();
                        }
                    
                        timeTaken = new Date().getTime()-startTime.getTime();
                        log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );

                        dataobjectInfoList = new ArrayList<>();
                        unSentCount = 0;
                    }
                    catch(DataServiceException de)
                    {
                        log.error(" DataServiceException de="+de.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(de));
                        lineProcessingInfo = de.getMessage();
                    }
                    catch(TException te)
                    {
                        log.error(" TException de="+te.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(te));
                        lineProcessingInfo = te.getMessage();
                    }
                     
                    if ( !lineProcessingInfo.isEmpty() )
                    {
                        processingResult = new HashMap<>();
                        processingResult.put("index", i);
                        processingResult.put("line", line+"                  ");
                        processingResult.put("lineProcessingInfo", lineProcessingInfo);

                        processingResults.add(processingResult);

                        errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,line,lineProcessingInfo));

                        failedLine++;                   
                        break;
                    }
                
                }                      
                                                              
                if ( jobId > 0 && processSucceedCount+duplicatedLine+skipedLine >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                {
                    dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                    dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);
                    dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), skipedLine);
                    
                    processSucceedCount = 0;
                    duplicatedLine = 0;
                    skipedLine = 0;
                    
                    while( commonService.isTooManyDataobjectInCache(organizationId) )
                    {
                        log.warn("too manay dataobject is in cache, stop collecting data! jobId="+jobId);
                        Tool.SleepAWhile(10, 0); 
                        dataService.setDatasourceEndJobTime(organizationId, jobId, JobTimeType.LAST_UPDATED_TIME.getValue());
                    }
                    
                    //log.info("not too manay dataobject is in cache, continue collecting data! jobId="+jobId);
                }
                
                if ( jobProcessStartRow <= 0 )
                {
                    if ( i == definitionProcessStartRow + processRowNumber && processMaxData==false )
                        break;
                }
                else
                {
                    if ( i == jobProcessStartRow + processRowNumber -1 && processMaxData==false )
                        break;                    
                }
            }
               
            if ( unSentCount > 0 )
            {
                startTime = new Date();
                lineProcessingInfo = "";

                try
                {
                    dataService.storeDataobjects(organizationId,targetRepositoryType,targetRepositoryId,dataobjectInfoList,jobId,true);

                    timeTaken = new Date().getTime()-startTime.getTime();
                    log.info(String.format("111 stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );
                }
                catch(DataServiceException de)
                {
                    log.error(" DataServiceException de="+de.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(de));
                    lineProcessingInfo = de.getMessage();
                }
                catch(TException te)
                {
                    log.error(" TException de="+te.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(te));
                    lineProcessingInfo = te.getMessage();
                }

                if ( !lineProcessingInfo.isEmpty() )
                {
                    processingResult = new HashMap<>();
                    processingResult.put("index", i);
                    processingResult.put("line", line+"                  ");
                    processingResult.put("lineProcessingInfo", lineProcessingInfo);

                    processingResults.add(processingResult);

                    errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,line,lineProcessingInfo));
                    failedLine++;            
                }
            } 
                          
            if ( jobId > 0 && failedLine==0 && (processSucceedCount>0 || duplicatedLine>0 || skipedLine>0) )
            {
                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);
                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), skipedLine);
            }
                    
            if ( jobId > 0 )
                dataService.setDatasourceEndJobTime(organizationId, jobId,JobTimeType.END_TIME.getValue());        
            
            in.close();
        }
        catch(Exception e)
        {
            log.error("importTxtFileContentToSystem() failed! e="+e+" last processing lineindex ="+i+" line="+line);
            errorInfo.append(String.format(" importTxtFileToSystem() failed! e=%s statcktrace=%s",e,ExceptionUtils.getStackTrace(e)));
        }
        finally 
        {
            try
            {
                if ( in != null) {
                    in.close();
                }

               // if (sc != null) {
               //     sc.close();
               // }
            }
            catch(Exception e) {
            }
        }
        
        if ( errorInfo.toString().isEmpty() )
            status = TaskJobStatus.COMPLETE.getValue();
        else
            status = TaskJobStatus.FAILED.getValue();
                
        importResults.put("status", status);
        
        importResults.put("processingResults", processingResults);
        importResults.put("totalProcessedLine", processedLine);
        importResults.put("skipedLine", skipedLine);
        importResults.put("failedLine", failedLine);
        importResults.put("errorInfo", errorInfo.toString());
         
        return importResults;
    }
    
    public static Map<String,Object> importExcelFileToSystem(boolean onlyDoValidation,int organizationId,long jobId,int dataValidationBehaviorTypeId,String filename,InputStream in,int definitionProcessStartRow, int jobProcessStartRow, boolean processMaxData,int processRowNumber,DataService.Client dataService,CommonService.Client commonService,StructuredFileDefinition fileDefinition,int datasourceType, int datasourceId, int sourceApplicationId,int targetRepositoryType,int targetRepositoryId,ClassWithCallBack callback,DataProcessingExtensionBase dataProcessingExtension, int alreadyProcessedLine,boolean needToCheckRecordDuplication,Counter serviceCounter)
    {
        int i=0;
        int processSucceedCount = 0;
        int unSentCount = 0;
        int processedLine = 0;
        int skipedLine = 0;
        int failedLine = 0;
        int duplicatedLine = 0;
        Row row;
        int status = 0;
        Sheet sheet;
        String primaryKeyValue;
        
        Map<String,Object> importResults = new HashMap<>();
        Map<String,Object> processingResult;
        List<Map<String,Object>> processingResults= new ArrayList();
        List<String> columns;

        List<Map<String,Object>> columnDefinitions;
        Map<String,Object> lineValidationResult;
        String lineValidationInfo;
        Map<Integer,String> columnDefaultValues;
        StringBuilder errorInfo = new StringBuilder();
        String lineProcessingInfo = "";
        Map<String,String> primaryKeyMap = new HashMap<>();
        String line = null;
                          
        Map<String,String> lineMap = new HashMap<>();
        String lineHash;
        Date startTime;
        long timeTaken;
        List<DataobjectInfo> dataobjectInfoList = new ArrayList<>();
        Date startTime1 = new Date();
        
        try
        {
            columnDefinitions = StructuredDataFileUtil.getColumnDefinitionsFromXmlStr(fileDefinition.getColumns());
            List<Integer> primaryKeyFieldIds = Util.getPrimaryKeyFieldIds(fileDefinition.getColumns());
            
            Map<String,Object> context = new HashMap<>();
            context.put("columnDefinitions", columnDefinitions);
            
            if ( dataProcessingExtension != null )
                dataProcessingExtension.setContext(context);
            
            //Create Workbook instance for xlsx/xls file input stream
            Workbook workbook = null;

            if(filename.toLowerCase().endsWith("xlsx"))
                workbook = new XSSFWorkbook(in);
            else 
            if(filename.toLowerCase().endsWith("xls"))
                workbook = new HSSFWorkbook(in);

            //Get the nth sheet from the workbook
            if ( fileDefinition.getExcelSheetName() == null || fileDefinition.getExcelSheetName().trim().isEmpty() )
                sheet = workbook.getSheetAt(0);
            else
                sheet = workbook.getSheet(fileDefinition.getExcelSheetName().trim());
            
            int columnNumber = FileUtil.getSheetMaxColumnNumber(sheet);
            
            //every sheet has rows, iterate over them
            Iterator<Row> rowIterator = sheet.iterator();

            if ( jobId > 0 )
                dataService.setDatasourceEndJobTime(organizationId, jobId,JobTimeType.START_TIME.getValue());
                        
            int k = 0;
            
            while (rowIterator.hasNext() )
            {
                i++;
                
                if ( callback != null )
                    callback.sendBackData(i);      
                
                //Get the row object
                row = rowIterator.next(); 

                if ( i < definitionProcessStartRow )
                    continue;
                
                if ( jobProcessStartRow > 0 && i < jobProcessStartRow )
                    continue;

                columns = new ArrayList<>();
                
                DataFormatter formatter = new DataFormatter();
           
                String strValue;
                    
                for (int j = 0; j < columnNumber; j++) 
                {
                    try
                    {
                        strValue = formatter.formatCellValue(row.getCell(j));
                    }
                    catch(Exception e)
                    {
                        log.warn("get column data failed! e="+e);
                        strValue = "";
                    }
                    
                    columns.add(strValue);
                }
 
                if ( columns.isEmpty() )
                    continue;

                line = "";
                for(String str : columns)
                    line+=str;

                log.info(" read line=("+line+") column size="+columns.size());
                
                if ( line.trim().isEmpty() )
                    continue;
                                   
                k++;
                
                if ( k <= alreadyProcessedLine )
                {
                    if ( needToCheckRecordDuplication )
                    {
                        lineHash = DataIdentifier.generateHash(line);
                        lineMap.put(lineHash, String.valueOf(i));

                        primaryKeyValue = Util.getPrimaryKeyValue(primaryKeyFieldIds,fileDefinition,columns.toArray(new String[1]),lineHash);
                        primaryKeyMap.put(primaryKeyValue, String.valueOf(i));
                    }
             
                    log.info("already processed. skip!");
                    continue;
                }
       
                lineHash = DataIdentifier.generateHash(line);
                
                if ( needToCheckRecordDuplication )
                {
                    String val = lineMap.get(lineHash);
                    if ( val != null )
                    {
                        processedLine++;
                        duplicatedLine++;
                        log.warn(String.format("line %d is duplicated with line %s!!!! filename=%s line=(%s)",i,val,filename,line));

                        if ( duplicatedLine >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                        {
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);
                            duplicatedLine = 0;
                        }

                        continue;
                    }

                    lineMap.put(lineHash, String.valueOf(i));
                }
                
                lineValidationResult = validateLine(columns.toArray(new String[1]),fileDefinition,columnDefinitions);
                
                lineValidationInfo = (String)lineValidationResult.get("lineValidationInfo");
                columnDefaultValues = (Map<Integer,String>)lineValidationResult.get("columnDefaultValues");
                
                if ( !lineValidationInfo.isEmpty() ) // invalid
                {
                    processingResult = new HashMap<>();
                    processingResult.put("index", i);
                    processingResult.put("line", columns.toString());
                    processingResult.put("lineProcessingInfo", lineValidationInfo);
                    processingResults.add(processingResult);
                    
                    errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,columns.toString(),lineValidationInfo));
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue() )
                    {
                        failedLine++;
                        
                        //if ( jobId > 0 )
                        //    dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);
                                                
                        break;
                    }
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SKIP_RECORD_WHEN_INVALID.getValue() )
                    {
                        skipedLine++;
                        
                        if ( jobId > 0 )
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), 1);
                                                
                        continue;
                    }                    
                }
                
                primaryKeyValue = Util.getPrimaryKeyValue(primaryKeyFieldIds,fileDefinition,columns.toArray(new String[1]),lineHash);
                    
                if ( needToCheckRecordDuplication )
                {
                    String keyVal = primaryKeyMap.get(primaryKeyValue);
                    if ( keyVal != null )
                    {
                     /*   if ( !primaryKeyFieldIds.isEmpty()  )  // 有主键表
                        {
                            errorInfo.append("Duplicated parimaryKey! key="+primaryKeyValue+" duplicated with line="+keyVal+" current line index="+i+" add linehash as key!");
                            log.error(errorInfo);                        

                            break;
                        }
                        else // 无主键表
                        {*/
                            processedLine++;
                            log.warn(String.format("Duplicated line:  %d is duplicated with line %s!!!! line=(%s) filename=%s",i,keyVal,line,filename));
                            duplicatedLine++;

                            if ( duplicatedLine >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                            {
                                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);
                                duplicatedLine = 0;
                            }

                            continue;
                        //}
                    }

                    primaryKeyMap.put(primaryKeyValue, String.valueOf(i));
                }
                
                if ( onlyDoValidation )
                {
                    processedLine++;
                    continue;
                }
                
                lineProcessingInfo = processLine(dataobjectInfoList,i,primaryKeyValue,columns.toArray(new String[1]),columnDefaultValues,fileDefinition,false,primaryKeyFieldIds,sourceApplicationId,datasourceType,datasourceId,targetRepositoryId, columnDefinitions, dataService, ColumnSeparatorType.COMMA.getSeparator(),dataProcessingExtension,jobId);
                    
                if ( !lineProcessingInfo.isEmpty() )
                {  
                    processingResult = new HashMap<>();
                    processingResult.put("index", i);
                    processingResult.put("line", columns.toString() +"                  ");
                    processingResult.put("lineProcessingInfo", lineProcessingInfo);
  
                    processingResults.add(processingResult);
                                        
                    errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,columns.toString(),lineProcessingInfo));
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue() )
                    {
                        failedLine++;
                        
                        //if ( jobId > 0 )
                        //    dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);
                                                
                        break;
                    }
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SKIP_RECORD_WHEN_INVALID.getValue() )
                    {
                        skipedLine++;
                        
                        if ( jobId > 0 )
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), 1);
                                                
                        continue;
                    }
                    
                    if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SET_DEFAULT_VALUET_TO_INVALID_COLUMN.getValue() )
                    {
                        failedLine++;
                        
                        if ( jobId > 0 )
                            dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);
                                                
                        continue;
                    }                    
                }
                                
                processedLine++;
                processSucceedCount++;
                unSentCount++;
                
                if ( unSentCount >= CommonKeys.DATASOURCE_END_TASK_DATAOBJECT_BATCH_NUM ||
                        jobId > 0 && processSucceedCount >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                {
                    startTime = new Date();
                    lineProcessingInfo = "";
                    
                    try
                    {
                        dataService.storeDataobjects(organizationId,targetRepositoryType,targetRepositoryId,dataobjectInfoList,jobId,true);
                        
                        timeTaken = new Date().getTime()-startTime.getTime();
                        log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );

                         if ( serviceCounter != null )
                        {
                            serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_COUNT, unSentCount);
                            timeTaken = new Date().getTime()-startTime1.getTime();
                            serviceCounter.increase(CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_TIME_TAKEN, timeTaken); 

                            startTime1 = new Date();
                        }
                         
                        dataobjectInfoList = new ArrayList<>();
                        unSentCount = 0;
                    }
                    catch(DataServiceException de)
                    {
                        log.error(" DataServiceException de="+de.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(de));
                        lineProcessingInfo = de.getMessage();
                    }
                    catch(TException te)
                    {
                        log.error(" TException de="+te.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(te));
                        lineProcessingInfo = te.getMessage();
                    }
                     
                    if ( !lineProcessingInfo.isEmpty() )
                    {
                        processingResult = new HashMap<>();
                        processingResult.put("index", i);
                        processingResult.put("line", line+"                  ");
                        processingResult.put("lineProcessingInfo", lineProcessingInfo);

                        processingResults.add(processingResult);

                        errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,line,lineProcessingInfo));

                        if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.QUIT_PROCESS_IMMEDIATELY_WHEN_INVALID.getValue() )
                        {
                            failedLine++;                   
                            break;
                        }

                        if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SKIP_RECORD_WHEN_INVALID.getValue() )
                        {
                            skipedLine++;                      
                            continue;
                        }

                        if ( dataValidationBehaviorTypeId == DataValidationBehaviorType.SET_DEFAULT_VALUET_TO_INVALID_COLUMN.getValue() )
                        {
                            failedLine++;

                            if ( jobId > 0 )
                                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.FAILED_ITEMS.getValue(), 1);

                            continue;
                        }
                    }
                }                      
                
                if ( jobId > 0 && processSucceedCount >= CommonKeys.DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM )
                {
                    dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
                    dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);

                    duplicatedLine = 0;
                    processSucceedCount = 0;
                    
                    while( commonService.isTooManyDataobjectInCache(organizationId) )
                    {
                        log.warn("too manay dataobject is in cache, stop collecting data! jobId="+jobId);
                        Tool.SleepAWhile(10, 0); 
                        dataService.setDatasourceEndJobTime(organizationId, jobId, JobTimeType.LAST_UPDATED_TIME.getValue());
                    }
                    
                    log.info("not too manay dataobject is in cache, continue collecting data! jobId="+jobId);                    
                }
                                           
                if ( jobProcessStartRow <= 0 )
                {
                    if ( i == definitionProcessStartRow + processRowNumber && processMaxData==false )
                        break;
                }
                else
                {
                    if ( i == jobProcessStartRow + processRowNumber - 1 && processMaxData==false )
                        break;                    
                }
            }
                        
            if ( unSentCount > 0 )
            {
                startTime = new Date();
                lineProcessingInfo = "";

                try
                {
                    dataService.storeDataobjects(organizationId,targetRepositoryType,targetRepositoryId,dataobjectInfoList,jobId,true);

                    timeTaken = new Date().getTime()-startTime.getTime();
                    log.info(String.format("stored dataobjects! size=[%d] took [%d] ms",dataobjectInfoList.size(),timeTaken) );
                }
                catch(DataServiceException de)
                {
                    log.error(" DataServiceException de="+de.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(de));
                    lineProcessingInfo = de.getMessage();
                }
                catch(TException te)
                {
                    log.error(" TException de="+te.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(te));
                    lineProcessingInfo = te.getMessage();
                }

                if ( !lineProcessingInfo.isEmpty() )
                {
                    processingResult = new HashMap<>();
                    processingResult.put("index", i);
                    processingResult.put("line", line+"                  ");
                    processingResult.put("lineProcessingInfo", lineProcessingInfo);

                    processingResults.add(processingResult);

                    errorInfo.append(String.format("index=%d,line=(%s)\n lineProcessingInfo=(%s)\n\n",i,line,lineProcessingInfo));
                    failedLine++;            
                }
            } 
            if ( jobId > 0 )
                dataService.setDatasourceEndJobTime(organizationId, jobId,JobTimeType.END_TIME.getValue());
                        
            if ( jobId > 0  && failedLine==0 && (processSucceedCount > 0 || duplicatedLine>0) )
            {
                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.DUPLICATE_ITEMS.getValue(), duplicatedLine);
                dataService.increaseDatasourceEndJobItemNumber(organizationId, jobId, JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), processSucceedCount);
            }
            
            in.close();
        }
        catch(Exception e)
        {
            log.error("111111 importExcelFileContentToSystem() failed! e="+e+", statcktrace="+ExceptionUtils.getStackTrace(e));
            errorInfo.append(String.format(" importExcelFileToSystem() failed! e=%s",ExceptionUtils.getStackTrace(e)));
        }
        
        if ( errorInfo.toString().isEmpty() )
            status = TaskJobStatus.COMPLETE.getValue();
        else
            status = TaskJobStatus.FAILED.getValue();
                
        importResults.put("status", status);
         
        importResults.put("processingResults", processingResults);
        importResults.put("totalProcessedLine", processedLine);
        importResults.put("skipedLine", skipedLine);
        importResults.put("failedLine", failedLine);
        importResults.put("errorInfo", errorInfo.toString());
                
        return importResults;
    }

    private static String processLine(List<DataobjectInfo> dataobjectInfoList,int lineIndex,String primaryKeyValue,String[] columns,Map<Integer,String> columnDefaultValues,StructuredFileDefinition fileDefinition, boolean removeAroundQuotation, List<Integer> primaryKeyFieldIds, int sourceApplicationId, int datasourceType, int datasourceId, int targetRepositoryId, List<Map<String, Object>> columnDefinitions, DataService.Client dataService, String seperator,DataProcessingExtensionBase dataProcessingExtension,long jobId)
    {
        List<FrontendMetadata> metadataList;
        Map<String, Object> columnDefinition;
        String targetDataobjectMetadataName;
        String datatimeFormat;
        FrontendMetadata metadata;
        String lineProcessingInfo = "";
                
        try 
        {                                
            DataobjectInfo obj = new DataobjectInfo();
            
            int newDatasourceId = 0;
            
            String dataobjectId = DataIdentifier.generateDataobjectId(fileDefinition.getOrganizationId(),sourceApplicationId,datasourceType,newDatasourceId,primaryKeyValue,targetRepositoryId);

            log.info("line number="+lineIndex+" dataobject id="+dataobjectId+" primaryKeyValue="+primaryKeyValue+" organization="+fileDefinition.getOrganizationId()
                    +"sourceApplicationId="+sourceApplicationId+" datasourceType="+datasourceType+" datasourceId="+newDatasourceId+" repositoryId="+targetRepositoryId);

            obj.setDataobjectId(dataobjectId);
            obj.setSourceApplicationId(sourceApplicationId);
            obj.setDatasourceType(datasourceType);
            obj.setDatasourceId(datasourceId);
            obj.setName(primaryKeyValue.substring(primaryKeyValue.indexOf(".")+1));
            obj.setSourcePath(primaryKeyValue);
            obj.setDataobjectType(fileDefinition.getMappingDataobjectTypeId());
            obj.setMetadataList(new ArrayList<FrontendMetadata>());
            obj.setIsPartialUpdate(false);
            obj.setNeedToSaveImmediately(false);

            metadataList = obj.getMetadataList();

            int fieldNo = 0;

            for(String field:columns)
            {                
                fieldNo++;
             
                columnDefinition = getColumnDefinition(columnDefinitions,fieldNo);

                if ( columnDefinition == null )
                     continue;

                boolean  needToProcess = (Boolean)columnDefinition.get("needToProcess");

                if ( needToProcess == false )
                    continue;

                if ( fileDefinition.getCreateDataobjectType() == 1)
                    targetDataobjectMetadataName = String.format("%s_%s",fileDefinition.getName().trim(),(String)columnDefinition.get("name"));
                else
                    targetDataobjectMetadataName = (String)columnDefinition.get("dataobjectTypeFieldName");

                if ( targetDataobjectMetadataName == null || targetDataobjectMetadataName.isEmpty() )
                {
                    log.debug(" target dataobjectmetadataName not found!  fieldNo="+fieldNo);
                    continue;
                }

                boolean processedByExtension = (Boolean)columnDefinition.get("processedByExtension");
                
                if ( processedByExtension == false )  // normal one
                {
                    String defaultValue = columnDefaultValues.get(fieldNo);
                    if ( defaultValue != null ) // invalide column
                    {
                        field = defaultValue;
                    }

                    boolean valueNeedToBeConvertedToKey = (Boolean)columnDefinition.get("needToBeConvertedToKey");

                    if ( valueNeedToBeConvertedToKey )
                    {
                        int codeId = Integer.parseInt((String)columnDefinition.get("codeId"));

                        field = dataService.convertValueToCodeKey(fileDefinition.getOrganizationId(),codeId,field);
                    }

                    int dataType = Integer.parseInt((String)columnDefinition.get("dataType"));

                    if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue() )
                    {
                        datatimeFormat = (String)columnDefinition.get("datetimeFormat");
                     
                        if ( datatimeFormat == null || datatimeFormat.trim().isEmpty() )
                            field = "";
                        else
                        {
                            field = field.trim();
                            
                            if ( field.length() < datatimeFormat.length() )
                                datatimeFormat = datatimeFormat.substring(0,field.length());
                        
                            Date date = Tool.convertDateStringToDateAccordingToPattern(field, datatimeFormat);

                            if ( date == null )
                                field = "";
                            else
                                field = String.valueOf(date.getTime());
                        }
                    }                          
                }
                   
                metadata = new FrontendMetadata(targetDataobjectMetadataName,true);
                metadata.setSingleValue(field);
                metadataList.add(metadata);
            }

            metadataList = obj.getMetadataList();
            metadata = new FrontendMetadata("stored_yearmonth",true);
            metadata.setSingleValue(Tool.getYearMonthStr(new Date()));
            metadataList.add(metadata);
            metadata = new FrontendMetadata("source_application_id",true);
            metadata.setSingleValue( String.valueOf(sourceApplicationId) );
            metadataList.add(metadata);
            metadata = new FrontendMetadata("datasource_type",true);
            metadata.setSingleValue( String.valueOf(datasourceType) );
            metadataList.add(metadata);
            metadata = new FrontendMetadata("datasource_id",true);
            metadata.setSingleValue( String.valueOf(datasourceId) );
            metadataList.add(metadata);
            
            if ( dataProcessingExtension != null )
                dataProcessingExtension.dataProcessing(obj, null, null);
            
            obj.setSize(0);
                               
            dataobjectInfoList.add(obj);
        }
        catch(Exception e)
        {
            log.error("processLine() failed! e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e));
            lineProcessingInfo = e.getMessage();
        }
     
        return lineProcessingInfo;
    }
    
    public static String[] preProcessLine(String line,boolean removeAroundQuotation,String separator,String replaceSeparator)
    {
        boolean isEndWithEmpty;
        
        line = Tool.removeSpecialChar(line);
        
        if ( removeAroundQuotation )
            line = Util.removeUnExpectedChar(line,separator,replaceSeparator);
        
        if ( line.endsWith(",") )
        {
            line+= ",edf";
            isEndWithEmpty = true;
        }
        else
            isEndWithEmpty = false;
        
        String columns[] = line.split(separator);
     
        for(int i=0;i<columns.length;i++)
        {
            if ( removeAroundQuotation )
                columns[i] = Tool.removeAroundQuotation(columns[i]);
            
            columns[i] = columns[i].trim();
        }
        
        if ( isEndWithEmpty )
        {
            String newColumns[] = new String[columns.length-1];
            
            for(int i=0;i<columns.length-1;i++)
               newColumns[i] = columns[i];
            
            return newColumns;
        }

        return columns;
    }
    
    public static Map<String,Object> validateLine(String[] columns,StructuredFileDefinition fileDefinition,List<Map<String,Object>> columnDefinitions)
    {
        Map<String,Object> validationResult = new HashMap<>();
        Map<Integer,String> columnDefaultValues = new HashMap<>();
        String lineValidationInfo = "";
        
        try 
        {
            int columnNo = 0;
            
            if ( columns.length != columnDefinitions.size() ) 
            {
                lineValidationInfo += Util.getBundleMessage("number_of_column_and_column_definition_not_match",columns.length,columnDefinitions.size());
            }
            else
            {
                for(String column:columns)
                {
                    columnNo++;

                    Map<String,Object> columnDefinition = getColumnDefinition(columnDefinitions,columnNo);

                    if ( columnDefinition == null )
                    {
                        lineValidationInfo += Util.getBundleMessage("column_definition_not_found",columnNo,column);
                        continue;
                    }

                    boolean needToProcess = (Boolean)columnDefinition.get("needToProcess");
                    if ( needToProcess == false )
                        continue;

                    boolean processedByExtension = (Boolean)columnDefinition.get("processedByExtension");
                    if ( processedByExtension == true )
                        continue;

                    boolean needToValidateData = (Boolean)columnDefinition.get("needToValidateData");

                    if ( needToValidateData )
                    {
                        //String columnValidationInfo = validateColumn(columnNo, column,columnDefinition);

                        String columnValidationInfo = "";
                        
                        if ( !columnValidationInfo.isEmpty() ) // invalid column
                        {
                            lineValidationInfo += columnValidationInfo;

                            String defaultValue = columnValidationInfo.substring("DefaultValue:".length(),columnValidationInfo.indexOf(","));
                            columnDefaultValues.put(columnNo, defaultValue);
                        }
                    }
                }                
            }
        }
        catch(Exception e)
        {
            lineValidationInfo = "validateLine() failed! e="+e;
        }
        
        validationResult.put("lineValidationInfo", lineValidationInfo);
        validationResult.put("columnDefaultValues",columnDefaultValues);

        return validationResult;
    }
    
    private static String validateColumn(int columnNo, String columnValueStr,Map<String,Object> columnDefinition)
    {
        int valueRangeCheckTypeId;
        String columnValidationInfo = "";
        String defaultValue = "";
        String columnName = "";
        String datetimeFormat;
        int dataType;
        Date date;
        boolean valid = false;
         
        try 
        {
            columnName = (String)columnDefinition.get("name");
            dataType = Integer.parseInt((String)columnDefinition.get("dataType"));
            datetimeFormat = (String)columnDefinition.get("datetimeFormat");

            if ( dataType == MetadataDataType.DATE.getValue() )
                defaultValue = Tool.convertDateToString((Date)columnDefinition.get("dateDefaultValue"), datetimeFormat);
            else
             if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                defaultValue = Tool.convertDateToString((Date)columnDefinition.get("timestampDefaultValue"), datetimeFormat);
            else               
                defaultValue = (String)columnDefinition.get("defaultValue");

            boolean nullIsAllowed =  (Boolean)columnDefinition.get("nullIsAllowed");

            while(true)
            {
                if ( nullIsAllowed == false ) // null is not allowed
                {
                    if ( columnValueStr == null || columnValueStr.isEmpty() )
                    {
                        columnValidationInfo = Util.getBundleMessage("validation_error_null_is_not_allowed",columnNo);
                        break;
                    }
                }

                Object data = Util.getValueObject(columnValueStr,dataType,datetimeFormat);

                if ( data == null )  // check data type
                {
                    columnValidationInfo = Util.getBundleMessage("validation_error_data_type_conversion_error",columnValueStr,MetadataDataType.findByValue(dataType).name());
                    break;
                }

                try {
                    valueRangeCheckTypeId = Integer.parseInt((String)columnDefinition.get("valueRangeCheckTypeId"));
                }
                catch(Exception e)
                {
                    log.debug(" no valueRangeCheckType found!");
                    break;
                }

                if ( ValueRangeCheckType.findByValue(valueRangeCheckTypeId) == ValueRangeCheckType.NOT_CHECK_RANGE )
                    break;

                if ( ValueRangeCheckType.findByValue(valueRangeCheckTypeId) == ValueRangeCheckType.FROM_ENUMERATION )
                {
                    String valueEnum = (String)columnDefinition.get("valueEnum");
                    String[] values = valueEnum.split("\\,");

                    for(String value : values)
                    {
                        if ( value.trim().equals(columnValueStr) )
                        {
                            valid = true;
                            break;
                        }
                    }

                    if ( !valid )  // not in enum 
                    {
                        columnValidationInfo = Util.getBundleMessage("validation_error_value_is_not_in_enum_list",columnValueStr,valueEnum);
                        break;
                    }
                }
                else
                if ( ValueRangeCheckType.findByValue(valueRangeCheckTypeId) == ValueRangeCheckType.FROM_CODEING_SYSTEM )
                {

                }
                else
                if ( ValueRangeCheckType.findByValue(valueRangeCheckTypeId) == ValueRangeCheckType.FROM_X_TO_Y )
                {
                    if ( dataType == MetadataDataType.DATE.getValue())
                    {
                        date = (Date)data;
                        
                        Date dateFromValue = (Date)columnDefinition.get("dateFromValue");
                        Date dateToValue = (Date)columnDefinition.get("dateToValue");
                        
                        if ( !(date.equals(dateFromValue) || date.equals(dateToValue) || date.after(dateFromValue) && date.before(dateToValue)) )
                        {
                            columnValidationInfo = Util.getBundleMessage("validation_error_value_is_not_in_range",columnValueStr,dateFromValue,dateToValue);
                            break;
                        }                           
                    }
                    else
                    if (dataType == MetadataDataType.TIMESTAMP.getValue() )
                    {
                        date = (Date)data;
                        
                        Date timestampFromValue = (Date)columnDefinition.get("timestampFromValue");
                        Date timestampToValue = (Date)columnDefinition.get("timestampToValue");
                        
                        if ( !(date.equals(timestampFromValue) || date.equals(timestampToValue) || date.after(timestampFromValue) && date.before(timestampToValue)) )
                        {
                            columnValidationInfo = Util.getBundleMessage("validation_error_value_is_not_in_range",columnValueStr,timestampFromValue,timestampToValue);
                            break;
                        }   
                    }
                    else                  
                    {
                        String fromValue = (String)columnDefinition.get("fromValue");
                        String toValue = (String)columnDefinition.get("toValue");
                        
                        if ( dataType == MetadataDataType.INTEGER.getValue() )
                        {
                            int intVal = (int)data;

                            if ( intVal >= Integer.parseInt(fromValue) && intVal <= Integer.parseInt(toValue) )
                                valid = true;
                        }
                        else
                        if ( dataType == MetadataDataType.LONG.getValue() )
                        {
                            long longVal = (long)data;

                            if ( longVal >= Long.parseLong(fromValue) && longVal <= Long.parseLong(toValue) )
                                valid = true;
                        }
                        else
                        if ( dataType == MetadataDataType.FLOAT.getValue() )
                        {
                            float floatVal = (float)data;
                            
                            if ( floatVal >= Float.parseFloat(fromValue) && floatVal <= Float.parseFloat(toValue) )
                                valid = true;
                        }
                        else
                        if ( dataType == MetadataDataType.DOUBLE.getValue() )
                        {
                            double doubleVal = (double)data;
                            
                            if ( doubleVal >= Double.parseDouble(fromValue) && doubleVal <= Double.parseDouble(toValue) )
                                valid = true;
                        }
                           
                        if ( !valid )
                        {
                            columnValidationInfo = Util.getBundleMessage("validation_error_value_is_not_in_range",columnValueStr,fromValue,toValue);
                            break;
                        }                           
                    }
                }

                break;
            }
        }
        catch(Exception e)
        {
            columnValidationInfo = Util.getBundleMessage("process_column_validation_error",columnName,columnValueStr,ExceptionUtils.getStackTrace(e));
        }
       
        if ( columnValidationInfo.isEmpty() )
            return "";
        else
            return String.format("DefaultValue:%s,ColumnNo:%d,Name:%s,Value:%s,Error:[%s]###############",defaultValue,columnNo,columnName,columnValueStr,columnValidationInfo);
    }
                 
    public static Map<String,Object> getColumnDefinition(List<Map<String, Object>> columnDefinitions, int columnNo) 
    {
         for(Map<String,Object> columnDefinition : columnDefinitions)
         {
             String id = (String)columnDefinition.get("id");
             
             if ( columnNo == Integer.parseInt(id) )
                 return columnDefinition;
         }
         
         return null;
    }
    
    public static Map<String,Object> getColumnDefinitionByName(List<Map<String, Object>> columnDefinitions, String columnName) 
    {
         for(Map<String,Object> columnDefinition : columnDefinitions)
         {
             String name = (String)columnDefinition.get("name");
             
             if ( columnName.equals(name) )
                 return columnDefinition;
         }
         
         return null;
    }    
    
    public static List<String> getColumnsWithUseAsEventTime(List<Map<String,Object>> definitions)
   {
        List<String> list = new ArrayList<>();
        
        for ( Map<String,Object> definition : definitions)
        {
            if ( definition.get("useAsEventTime") == null )
                continue;

            if ( (Boolean)definition.get("useAsEventTime") )
            {
                list.add((String)definition.get("name"));             
                log.info(" useAsEventTime is true ="+(String)definition.get("name"));
            }
        }
        
        return list;
   }
     
    public static int getNumberOfColumnWithUseAsEventTime(List<Map<String,Object>> definitions)
    {
        int count = 0;
         
        for ( Map<String,Object> definition : definitions)
        {
            if ( definition.get("useAsEventTime") == null )
                continue;
            
            if ( (Boolean)definition.get("useAsEventTime") )
                count++;                 
        }
        
        return count;
    }    
        
    public static String getXmlStrFromColumnDefinitions(List<Map<String,Object>> definitions) throws Exception
    {
        String NODES = "//structureDataFileDefinition";
        String NODE = "column";        
        String dateStr;
        String datetimeFormat;
        
        try
        {
            String INIT_XML  = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+"\n"+"<structureDataFileDefinition>\n"+"</structureDataFileDefinition>";
            
            SAXReader saxReader = new SAXReader();
            ByteArrayInputStream in = new ByteArrayInputStream(INIT_XML.getBytes());
            Document doc= saxReader.read(in);

            Element element = (Element)doc.selectSingleNode(NODES);

            for ( Map<String,Object> definition : definitions)
            {
                Element NodeElement = element.addElement(NODE);
                
                Element idElement = NodeElement.addElement("id");
                idElement.setText(definition.get("id")==null?"":definition.get("id").toString());
                
                Element needToProcessElement = NodeElement.addElement("needToProcess");
                needToProcessElement.setText(definition.get("needToProcess")==null?"0":((Boolean)definition.get("needToProcess"))==true?"1":"0");
               
                Element nameElement = NodeElement.addElement("name");
                nameElement.setText(definition.get("name")==null?"":(String)definition.get("name"));
                
                Element descriptionElement = NodeElement.addElement("description");
                descriptionElement.setText(definition.get("description")==null?"":(String)definition.get("description"));
                              
                log.info(" dataItemStandardId="+definition.get("dataItemStandardId"));
                Element dataItemStandardElement = NodeElement.addElement("dataItemStandardId");
                dataItemStandardElement.setText(definition.get("dataItemStandardId")==null?"":(String)definition.get("dataItemStandardId"));
                
                log.info(" metadataGroup="+definition.get("metadataGroup"));
                Element metadataGroupElement = NodeElement.addElement("metadataGroup");
                metadataGroupElement.setText(definition.get("metadataGroup")==null?"":(String)definition.get("metadataGroup"));
                
                Element dataTypeElement = NodeElement.addElement("dataType");
                dataTypeElement.setText(definition.get("dataType")==null?"":definition.get("dataType").toString());
                
                Element indexTypeElement = NodeElement.addElement("indexType");
                indexTypeElement.setText(definition.get("indexType")==null?"":definition.get("indexType").toString());
                
                Element datetimeFormatElement = NodeElement.addElement("datetimeFormat");
                datetimeFormat = definition.get("datetimeFormat")==null?"":(String)definition.get("datetimeFormat");
                datetimeFormatElement.setText(datetimeFormat);
                
                Element processedByExtensionElement = NodeElement.addElement("processedByExtension");
                processedByExtensionElement.setText(definition.get("processedByExtension")==null?"0":((Boolean)definition.get("processedByExtension"))==true?"1":"0");
                
                Element sizeElement = NodeElement.addElement("size");
                sizeElement.setText(definition.get("size")==null?"":definition.get("size").toString());
                
                Element precisionElement = NodeElement.addElement("precision");
                precisionElement.setText(definition.get("precision")==null?"":definition.get("precision").toString());
                
                Element isPrimaryKeyElement = NodeElement.addElement("isPrimaryKey");
                isPrimaryKeyElement.setText(definition.get("isPrimaryKey")==null?"0":((Boolean)definition.get("isPrimaryKey"))==true?"1":"0");

                Element useAsEventTimeElement = NodeElement.addElement("useAsEventTime");
                useAsEventTimeElement.setText(definition.get("useAsEventTime")==null?"0":((Boolean)definition.get("useAsEventTime"))==true?"1":"0");
                
                Element dataobjectTypeFieldNameElement = NodeElement.addElement("dataobjectTypeFieldName");
                dataobjectTypeFieldNameElement.setText(definition.get("dataobjectTypeFieldName")==null?"":(String)definition.get("dataobjectTypeFieldName")); 
                
                Element needToValidateDataElement = NodeElement.addElement("needToValidateData");
                needToValidateDataElement.setText(definition.get("needToValidateData")==null?"0":((Boolean)definition.get("needToValidateData"))==true?"1":"0");
                
                Element codeIdElement = NodeElement.addElement("codeId");
                codeIdElement.setText(definition.get("codeId")==null?"":definition.get("codeId").toString());       
                
                Element needToBeConvertedToKeyElement = NodeElement.addElement("needToBeConvertedToKey");
                needToBeConvertedToKeyElement.setText(definition.get("codeId")==null || definition.get("needToBeConvertedToKey")==null?"0":((Boolean)definition.get("needToBeConvertedToKey"))==true?"1":"0");    
                
                Element nullIsAllowedElement = NodeElement.addElement("nullIsAllowed");
                nullIsAllowedElement.setText(definition.get("nullIsAllowed")==null?"0":((Boolean)definition.get("nullIsAllowed"))==true?"1":"0");    
                                
                Element defaultValueElement = NodeElement.addElement("defaultValue");
                defaultValueElement.setText(definition.get("defaultValue")==null?"":(String)definition.get("defaultValue"));
                
                Element dateDefaultValueElement = NodeElement.addElement("dateDefaultValue");
                dateStr = Tool.convertDateToString((Date)definition.get("dateDefaultValue"), datetimeFormat);
                dateDefaultValueElement.setText(dateStr);
                
                Element timestampDefaultValueElement = NodeElement.addElement("timestampDefaultValue");
                dateStr = Tool.convertDateToString((Date)definition.get("timestampDefaultValue"), datetimeFormat);
                timestampDefaultValueElement.setText(dateStr);
                
                Element valueRangeCheckTypeElement = NodeElement.addElement("valueRangeCheckTypeId");
                valueRangeCheckTypeElement.setText(definition.get("valueRangeCheckTypeId")==null?"":definition.get("valueRangeCheckTypeId").toString());
                
                Element valueEnumElement = NodeElement.addElement("valueEnum");
                valueEnumElement.setText(definition.get("valueEnum")==null?"":(String)definition.get("valueEnum"));                
             
                Element fromValueElement = NodeElement.addElement("fromValue");
                fromValueElement.setText(definition.get("fromValue")==null?"":definition.get("fromValue").toString());
                
                Element toValueElement = NodeElement.addElement("toValue");
                toValueElement.setText(definition.get("toValue")==null?"":definition.get("toValue").toString());        
                
                Element dateFromValueElement = NodeElement.addElement("dateFromValue");
                dateStr = Tool.convertDateToString((Date)definition.get("dateFromValue"), datetimeFormat);
                dateFromValueElement.setText(dateStr);
                
                Element dateToValueElement = NodeElement.addElement("dateToValue");
                dateStr = Tool.convertDateToString((Date)definition.get("dateToValue"), datetimeFormat);
                dateToValueElement.setText(dateStr);                
                        
                Element timestampFromValueElement = NodeElement.addElement("timestampFromValue");
                dateStr = Tool.convertDateToString((Date)definition.get("timestampFromValue"), datetimeFormat);
                timestampFromValueElement.setText(dateStr);
                
                Element timestampToValueElement = NodeElement.addElement("timestampToValue");
                dateStr = Tool.convertDateToString((Date)definition.get("timestampToValue"), datetimeFormat);
                timestampToValueElement.setText(dateStr);
                
                Element selectFromElement = NodeElement.addElement("selectFrom");
                selectFromElement.setText(definition.get("selectFrom")==null?"":(String)definition.get("selectFrom"));
            }
            
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            OutputFormat format = OutputFormat.createPrettyPrint(); 
           // OutputFormat format = new OutputFormat();
 
            XMLWriter output = new XMLWriter(out,format);
            output.write(doc);

            return out.toString("UTF-8");
        }
        catch(Exception e)
        {
            throw e;
        }
    }    
        
    public static List<Map<String,Object>> getColumnDefinitionsFromXmlStr(String xmlStr) throws Exception
    {
        Map<String,Object> definition;        
        String nodeStr = "//structureDataFileDefinition/column";
        Date date;
        String datetimeFormat;
        
        List<Map<String,Object>> definitions = new ArrayList<>();
        
        try
        {
            Document metadataXmlDoc = Tool.getXmlDocument(xmlStr);
            List<Element> nodes = metadataXmlDoc.selectNodes(nodeStr);

            for( Element node: nodes)
            {
                definition = new HashMap<>();
                
                definition.put("id", node.element("id").getTextTrim());
                definition.put("needToProcess", node.element("needToProcess").getTextTrim().equals("1"));
                definition.put("name", node.element("name").getTextTrim());
                definition.put("selectFrom", node.element("selectFrom")==null?"":node.element("selectFrom").getTextTrim());
                definition.put("description", node.element("description").getTextTrim());
                definition.put("dataType", node.element("dataType").getTextTrim());
                definition.put("dataItemStandardId",node.element("dataItemStandardId")==null?0:Integer.parseInt(node.element("dataItemStandardId").getTextTrim()));
                
                definition.put("indexType", node.element("indexType")==null?"":node.element("indexType").getTextTrim());
                
                datetimeFormat = node.element("datetimeFormat")==null?"": node.element("datetimeFormat").getTextTrim();
                definition.put("datetimeFormat",datetimeFormat);
                
                definition.put("processedByExtension", node.element("processedByExtension")==null?false:node.element("processedByExtension").getTextTrim().equals("1"));
                definition.put("size", node.element("size").getTextTrim()); 
                definition.put("precision", node.element("precision").getTextTrim());    
                definition.put("dataobjectTypeFieldName", node.element("dataobjectTypeFieldName")==null?"":node.element("dataobjectTypeFieldName").getTextTrim());    
                definition.put("isPrimaryKey", node.element("isPrimaryKey").getTextTrim().equals("1"));
                definition.put("useAsEventTime", node.element("useAsEventTime")==null?false:node.element("useAsEventTime").getTextTrim().equals("1"));
                definition.put("codeId", node.element("codeId")==null?"":node.element("codeId").getTextTrim());
                definition.put("needToBeConvertedToKey",node.element("needToBeConvertedToKey")==null?false:node.element("needToBeConvertedToKey").getTextTrim().equals("1"));
                                
                definition.put("needToValidateData", node.element("needToValidateData")==null?false:node.element("needToValidateData").getTextTrim().equals("1"));
                definition.put("nullIsAllowed",node.element("nullIsAllowed")==null?false:node.element("nullIsAllowed").getTextTrim().equals("1"));
                
                definition.put("defaultValue", node.element("defaultValue")==null?"":node.element("defaultValue").getTextTrim());
                
                date = Tool.convertDateStringToDateAccordingToPattern(node.element("dateDefaultValue")==null?"":node.element("dateDefaultValue").getTextTrim(),datetimeFormat);
                definition.put("dateDefaultValue",date );
                
                date = Tool.convertDateStringToDateAccordingToPattern(node.element("timestampDefaultValue")==null?"":node.element("timestampDefaultValue").getTextTrim(),datetimeFormat);
                definition.put("timestampDefaultValue",date );                
                
                definition.put("valueRangeCheckTypeId",node.element("valueRangeCheckTypeId")==null?"":node.element("valueRangeCheckTypeId").getTextTrim());
                definition.put("valueEnum", node.element("valueEnum")==null?"":node.element("valueEnum").getTextTrim());
                
                definition.put("fromValue", node.element("fromValue")==null?"":node.element("fromValue").getTextTrim());
                definition.put("toValue", node.element("toValue")==null?"":node.element("toValue").getTextTrim());
                
                date = Tool.convertDateStringToDateAccordingToPattern(node.element("dateFromValue")==null?"":node.element("dateFromValue").getTextTrim(),datetimeFormat);
                definition.put("dateFromValue",date );
                
                date = Tool.convertDateStringToDateAccordingToPattern(node.element("dateToValue")==null?"":node.element("dateToValue").getTextTrim(),datetimeFormat);
                definition.put("dateToValue",date );                
                
                date = Tool.convertDateStringToDateAccordingToPattern(node.element("timestampFromValue")==null?"":node.element("timestampFromValue").getTextTrim(),datetimeFormat);
                definition.put("timestampFromValue",date );
                
                date = Tool.convertDateStringToDateAccordingToPattern(node.element("timestampToValue")==null?"":node.element("timestampToValue").getTextTrim(),datetimeFormat);
                definition.put("timestampToValue",date );    
                
                definitions.add(definition);
            }
        }
        catch(Exception e)
        {
            log.error("getColumnDefinitionsFromXmlStr(String xmlStr) e="+e+ " xmlStr="+xmlStr);
            throw e;
        }
                    
        return definitions;
    }
}
