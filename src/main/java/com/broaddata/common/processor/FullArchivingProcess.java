package com.broaddata.common.processor;

import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.enumeration.DatabaseItemType;
import com.broaddata.common.model.enumeration.JobItemProcessingStatisticType;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.JDBCUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.codec.binary.Base64;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;
import java.io.InputStreamReader;
import javafx.util.Pair;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FullArchivingProcess {
    static final Logger log = Logger.getLogger("DatabaseProcessor");
    
    protected DataService.Client dataService = null;
    protected ComputingNode computingNode;
    protected Job job = null;
    //protected DatasourceType datasourceType = null;
    
    private DatasourceConnection datasourceConnection;
    private DatabaseType databaseType;
    private int databaseItemType;
    private String databaseItem;
    private String filter;
    private DatasourceConnection targetDatasourceConnection = null;
    private int targetDataobjectTypeId;
    private String schema;
    
    private static BlockingQueue<String> dataQueue; 
    private static volatile String[] childThreadStatus;
    private static volatile List<String> fileList;
    private static Lock lock;
    private String outputFolder;
    private String fullOutputControlFileName;
    private volatile long totalCount = 0l;
    private final String delimiter = "~";
    private final int PAGE_SIZE = 1000000; // 100万
    private final int FLUSH_LINE = 100000; // 10万
    private final int maxThreadNum = 10;
    private final boolean addEmptyEdfIdField=false;
    private final boolean addEdfLastModifyTimeField=true;
    //private final String tableNamePrefix = "";
    private final String folderNameForMaster = "/home/data";
    private final String skipErrorLine = null;
    private String EdfLastModifyTime;
    private String tableName;
    private String targetTableName;
    private String targetUser;
    private String tableSpace;
    private String indexSpace;
    private String faSchema;
    
    FullArchivingProcess(Job job, DataService.Client dataService, ComputingNode computingNode, DatasourceConnection datasourceConnection, DatabaseType databaseType, String schema, String databaseItem, int databaseItemType, String filter, DatasourceConnection targetDatasourceConnection, int targetDataobjectTypeId) {
        this.job = job;
        this.dataService = dataService;
        this.computingNode = computingNode;
        this.datasourceConnection = datasourceConnection;
        this.databaseType = databaseType;
        this.schema = schema;
        this.databaseItem = databaseItem;
        this.databaseItemType = databaseItemType;
        this.filter = filter;
        this.targetDatasourceConnection = targetDatasourceConnection;
        this.targetDataobjectTypeId = targetDataobjectTypeId;
    }
    
    public String processFullArchiving() throws Exception
    {
        DataobjectType dataobjectType;
        Connection dbConn = null;
         Connection targetDBConn = null;
        PreparedStatement targetStatement = null;
        ResultSet targetResultSet = null;
        String countQuerySQL = "";
        int count;
        StringBuilder errorInfo = new StringBuilder("");
        
        log.info("processFullArchiving() ...");
        int threadNum;
        
        if(computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue())
        {
            outputFolder = "c:\\tmpdata";
        }
        else 
        {
            outputFolder = "/edf/tmpdata";
        }
        File file =new File(outputFolder);  
        if  (!file.exists()  && !file.isDirectory()) file.mkdir();

        try
        {
            dbConn = JDBCUtil.getJdbcConnection(datasourceConnection);
            
            countQuerySQL = generateQuerySQLCount(databaseType,schema, databaseItemType, databaseItem, filter);
            count = getQueryCountOnly(dbConn,countQuerySQL);
            if (count == 0 ) return errorInfo.toString();
            
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), count);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 0);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), 0);

            dataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(targetDataobjectTypeId).array());
            
            tableName = databaseItem;
            targetTableName = dataobjectType.getName();
            targetUser = targetDatasourceConnection.getUserName();
            tableSpace = getDsConnPropValue(targetDatasourceConnection, "table_space");
            indexSpace = getDsConnPropValue(targetDatasourceConnection, "index_space");
            faSchema = getDsConnPropValue(targetDatasourceConnection, "schema");

            String gpMasterIP = getDsConnPropValue(targetDatasourceConnection, "ip_address");
            int gpMasterPort = 22;
            String gpMasterUser = "gpadmin";
            String gpMasterPassword = "gpadmin";
            String gpMasterPrivateKey = null;
            boolean gpMasterUsePassword = true;
            String gpTargetDB = getDsConnPropValue(targetDatasourceConnection, "database_name");
            String gpTargetDBPort = getDsConnPropValue(targetDatasourceConnection, "port");
            if (gpTargetDBPort.isEmpty()) gpTargetDBPort = DatabaseType.GREENPLUM.getDefaultPort();
            
            if(addEdfLastModifyTimeField) EdfLastModifyTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            fileList = new ArrayList();
            
            fullOutputControlFileName = String.format("%s/%s.sql",outputFolder,tableName);
            File fc = new File(fullOutputControlFileName);
            if ( !fc.exists() ) fc.createNewFile();
            fileList.add(fullOutputControlFileName);
            
            Writer controlFileWriter = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(fullOutputControlFileName),"UTF-8"));
            controlFileWriter.write(String.format("DROP TABLE IF EXISTS %s; \n\n",targetTableName));
            List<String> sqlList = Util.generateGreenPlumCreateTableSqlFromConnMetadata(dbConn,null,addEmptyEdfIdField,addEdfLastModifyTimeField, tableName,targetTableName,databaseType, tableSpace, indexSpace ,targetUser);
            for(String sql : sqlList) {
                controlFileWriter.write(sql);
            }
            controlFileWriter.write(String.format("\n"));
            controlFileWriter.close();
            
            int pageNum = count/PAGE_SIZE + 1;
            threadNum = pageNum > maxThreadNum ? maxThreadNum : pageNum;
            
            dataQueue = new ArrayBlockingQueue<>(pageNum);
            childThreadStatus = new String[threadNum];
            lock = new ReentrantLock();
            
            for(int i=0;i<threadNum;i++)
            {
                childThreadStatus[i] = "waiting";

                DBExporter dbExporter = new DBExporter(i,errorInfo);
                Thread thread = new Thread(dbExporter);
                thread.start();
            }
            
            for(int i=0;i<pageNum;i++) dataQueue.put(String.valueOf(i+1));
            
            Thread.sleep(15000);
            while(true)
            {
                int doneNum = 0;
                int errorNum = 0;

                for(int i=0;i<threadNum;i++)
                {
                    log.debug("check thread: no="+i+" status="+childThreadStatus[i]);

                    if ( childThreadStatus[i].equals("waiting") )  doneNum++;
                    if ( childThreadStatus[i].equals("error") )  errorNum++;
                }

                if ( doneNum == threadNum && dataQueue.isEmpty() )
                {
                    log.info("check thread: all done! doneNum="+doneNum);
                    break;
                }
                if ( errorNum > 0 )
                {
                    log.error("check thread: there are some errors!!!");
                    break;
                }

                log.info("check thread: has "+(threadNum-doneNum)+" processing job! totalCount="+totalCount+" queue size="+dataQueue.size());
                Tool.SleepAWhile(3, 0);
            }
            if (count == totalCount) {
                log.info("Export data succeeded !!!! totalCount="+totalCount);
            } else {
                errorInfo.append(String.format("Error break in EXP: Failed when export table(%s) to file(%s)!",databaseItem,outputFolder));
            }
            
            if (errorInfo.length() == 0) {                
                int successCnt = 0;
                for (String filePut: fileList) {
                    Pair<String, Boolean> retPairValue = null;
                    int retryCnt = 0;
                    while (retryCnt < 3) {
                        retPairValue = scpFile(filePut, folderNameForMaster, "PUT", gpMasterIP, gpMasterPort, gpMasterUser, gpMasterPassword, gpMasterPrivateKey, gpMasterUsePassword);
                        if(retPairValue.getValue()) {
                           break; 
                        } else {
                            retryCnt ++ ;
                            Thread.sleep(3000);
                        }
                    }
                    if (retryCnt == 3) {
                        errorInfo.append(String.format("Error break in SCP: Failed when put file (%s) to GP Master (IP=%s)!",filePut,gpMasterIP));
                        errorInfo.append(retPairValue.getKey());
                    }
                    else {
                        successCnt ++ ;
                        log.info("Put data to GP master (IP="+gpMasterIP+") folder ("+folderNameForMaster+") from local file ("+filePut+") succeeded !!!! Result: "+retPairValue.getKey());
                    }
                }
                if (successCnt == fileList.size()) {
                    log.info("Put data to GP master (IP="+gpMasterIP+") folder ("+folderNameForMaster+") from local folder ("+outputFolder+") succeeded !!!! fileCount="+successCnt);
                    
                    String cmd = String.format("psql -p %s -U %s -d %s -f %s/%s.sql",gpTargetDBPort, gpMasterUser, gpTargetDB, folderNameForMaster, databaseItem);
                    Pair<String, Boolean> retPairValue = null;
                    int retryCnt = 0;
                    while (retryCnt < 3) {
                        retPairValue = execShellScript(cmd, gpMasterIP, gpMasterPort, gpMasterUser, gpMasterPassword, gpMasterPrivateKey, gpMasterUsePassword);
                        if(retPairValue.getValue() && !retPairValue.getKey().contains(" ERROR:")) {
                           break; 
                        } else {
                            retryCnt ++ ;
                            Thread.sleep(3000);
                        }
                    }
                    if (retryCnt == 3) {
                        errorInfo.append(String.format("Error break in SCMD: Failed when execute command(%s) in GP Master (IP=%s)!",cmd,gpMasterIP));
                        errorInfo.append(retPairValue.getKey());
                    }
                    else log.info(String.format("Execute command(%s) in GP Master (IP=%s) succeedfully! Result: %s",cmd,gpMasterIP,retPairValue.getKey()));
                }
            }
            if (errorInfo.length() == 0) {
                String SQLCount = generateQuerySQLCount(DatabaseType.GREENPLUM, faSchema, DatabaseItemType.DB_TABLE.getValue(), targetTableName, null);
                targetDBConn = JDBCUtil.getJdbcConnection(targetDatasourceConnection);
                targetStatement = targetDBConn.prepareStatement(SQLCount);
                targetResultSet = JDBCUtil.executeQuery(targetStatement);
                targetResultSet.next();
                long targetCount = targetResultSet.getLong(1); 
                if (targetCount == totalCount) {
                    log.info("Copy data to GP succeeded !!!! totalCount="+totalCount);

                    String cmd = String.format("rm -Rf %s/%s*", folderNameForMaster, databaseItem);
                    execShellScript(cmd, gpMasterIP, gpMasterPort, gpMasterUser, gpMasterPassword, gpMasterPrivateKey, gpMasterUsePassword);
                    
                    for (String filePut: fileList) new File(filePut).delete();
                } else {
                    errorInfo.append(String.format("Error break in COPY: Failed when copy file(%s) data to GP table(%s)! Source row count (%d) != target row count (%d)",outputFolder,databaseItem,totalCount,targetCount));
                }
            }
        }
        catch (Exception e)
        {
            String errorStr = "processFullArchiving() failed! countQuerySQL=["+countQuerySQL+"] e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            errorInfo.append(errorStr);
            throw new Exception(errorStr);            
        }
        finally
        {
            JDBCUtil.close(targetResultSet);
            JDBCUtil.close(targetStatement);
            JDBCUtil.close(targetDBConn);
            JDBCUtil.close(dbConn);
        }
        return errorInfo.toString();
    }
    
    public String processFullArchivingRedo() throws Exception
    {
        StringBuilder errorInfo = new StringBuilder("");
        
        try {
            if(true) {
                //dataService.updateLastProcessedItem(PAGE_SIZE, PAGE_SIZE, databaseItem)
                //Todo: fileList
            }
            if(!processExport(errorInfo)) return errorInfo.toString();
            if(!processSCP(errorInfo)) return errorInfo.toString();
            processCopy(errorInfo);
        }
        catch (Exception e) {
            throw (e);
        }
        finally {
        }
        return errorInfo.toString();
    }

    public boolean processExport(StringBuilder errorInfo) throws Exception
    {
        DataobjectType dataobjectType;
        Connection dbConn = null;
        String countQuerySQL = "";
        int count;
        
        log.info("processExport() ...");
        int threadNum;
        
        if(computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue())
        {
            outputFolder = "c:\\tmpdata";
        }
        else 
        {
            outputFolder = "/edf/tmpdata";
        }
        File file =new File(outputFolder);  
        if  (!file.exists()  && !file.isDirectory()) file.mkdir();

        try
        {
            dbConn = JDBCUtil.getJdbcConnection(datasourceConnection);
            
            countQuerySQL = generateQuerySQLCount(databaseType,schema, databaseItemType, databaseItem, filter);
            count = getQueryCountOnly(dbConn,countQuerySQL);
            if (count == 0 ) return true;
            
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.TOTAL_ITEMS.getValue(), count);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), 0);
            dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.SKIPPED_ITEMS.getValue(), 0);

            dataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(targetDataobjectTypeId).array());
            
            tableName = databaseItem;
            targetTableName = dataobjectType.getName();
            targetUser = targetDatasourceConnection.getUserName();
            tableSpace = getDsConnPropValue(targetDatasourceConnection, "table_space");
            indexSpace = getDsConnPropValue(targetDatasourceConnection, "index_space");
            faSchema = getDsConnPropValue(targetDatasourceConnection, "schema");
      
            if(addEdfLastModifyTimeField) EdfLastModifyTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            fileList = new ArrayList();
            
            fullOutputControlFileName = String.format("%s/%s.sql",outputFolder,tableName);
            File fc = new File(fullOutputControlFileName);
            if ( !fc.exists() ) fc.createNewFile();
            fileList.add(fullOutputControlFileName);
            
            Writer controlFileWriter = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(fullOutputControlFileName),"UTF-8"));
            controlFileWriter.write(String.format("DROP TABLE IF EXISTS %s; \n\n",targetTableName));
            List<String> sqlList = Util.generateGreenPlumCreateTableSqlFromConnMetadata(dbConn,null,addEmptyEdfIdField,addEdfLastModifyTimeField, tableName,targetTableName,databaseType, tableSpace, indexSpace ,targetUser);
            for(String sql : sqlList) {
                controlFileWriter.write(sql);
            }
            controlFileWriter.write(String.format("\n"));
            controlFileWriter.close();
            
            int pageNum = count/PAGE_SIZE + 1;
            threadNum = pageNum > maxThreadNum ? maxThreadNum : pageNum;
            
            dataQueue = new ArrayBlockingQueue<>(pageNum);
            childThreadStatus = new String[threadNum];
            lock = new ReentrantLock();
            
            for(int i=0;i<threadNum;i++)
            {
                childThreadStatus[i] = "waiting";

                DBExporter dbExporter = new DBExporter(i,errorInfo);
                Thread thread = new Thread(dbExporter);
                thread.start();
            }
            
            for(int i=0;i<pageNum;i++) dataQueue.put(String.valueOf(i+1));
            
            Thread.sleep(15000);
            while(true)
            {
                int doneNum = 0;
                int errorNum = 0;

                for(int i=0;i<threadNum;i++)
                {
                    log.debug("check thread: no="+i+" status="+childThreadStatus[i]);

                    if ( childThreadStatus[i].equals("waiting") )  doneNum++;
                    if ( childThreadStatus[i].equals("error") )  errorNum++;
                }

                if ( doneNum == threadNum && dataQueue.isEmpty() )
                {
                    log.info("check thread: all done! doneNum="+doneNum);
                    break;
                }
                if ( errorNum > 0 )
                {
                    log.error("check thread: there are some errors!!!");
                    break;
                }

                log.info("check thread: has "+(threadNum-doneNum)+" processing job! totalCount="+totalCount+" queue size="+dataQueue.size());
                Tool.SleepAWhile(3, 0);
            }
            if (count == totalCount) {
                log.info("Export data succeeded !!!! totalCount="+totalCount);
                return true;
            } else {
                errorInfo.append(String.format("Error break in EXP: Failed when export table(%s) to file(%s)!",databaseItem,outputFolder));
                return false;
            }
        }
        catch (Exception e)
        {
            String errorStr = "processFullArchiving() failed! countQuerySQL=["+countQuerySQL+"] e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            errorInfo.append(errorStr);
            throw new Exception(errorStr);
        }
        finally
        {
            JDBCUtil.close(dbConn);
        }
    }
    
    public boolean processSCP(StringBuilder errorInfo) throws Exception
    {
        log.info("processSCP() ...");
        
        String gpMasterIP = getDsConnPropValue(targetDatasourceConnection, "ip_address");
        int gpMasterPort = 22;
        String gpMasterUser = "gpadmin";
        String gpMasterPassword = "gpadmin";
        String gpMasterPrivateKey = null;
        boolean gpMasterUsePassword = true;
            
        try {
            int successCnt = 0;
            for (String filePut: fileList) {
                Pair<String, Boolean> retPairValue = null;
                int retryCnt = 0;
                while (retryCnt < 3) {
                    retPairValue = scpFile(filePut, folderNameForMaster, "PUT", gpMasterIP, gpMasterPort, gpMasterUser, gpMasterPassword, gpMasterPrivateKey, gpMasterUsePassword);
                    if(retPairValue.getValue()) {
                       break; 
                    } else {
                        retryCnt ++ ;
                        Thread.sleep(3000);
                    }
                }
                if (retryCnt == 3) {
                    errorInfo.append(String.format("Error break in SCP: Failed when put file (%s) to GP Master (IP=%s)!",filePut,gpMasterIP));
                    errorInfo.append(retPairValue.getKey());
                }
                else {
                    successCnt ++ ;
                    log.info("Put data to GP master (IP="+gpMasterIP+") folder ("+folderNameForMaster+") from local file ("+filePut+") succeeded !!!! Result: "+retPairValue.getKey());
                }
            }
            if (successCnt == fileList.size()) {
                log.info("Put data to GP master (IP="+gpMasterIP+") folder ("+folderNameForMaster+") from local folder ("+outputFolder+") succeeded !!!! fileCount="+successCnt);
                return true;
            }
            else {
                return false;
            }
        }
        catch (Exception e) {
            String errorStr = "processSCP() failed! e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            throw new Exception(errorStr);
        }
    }
    
    public boolean processCopy(StringBuilder errorInfo) throws Exception
    {
        DataobjectType dataobjectType;
        Connection targetDBConn = null;
        PreparedStatement targetStatement = null;
        ResultSet targetResultSet = null;
        
        log.info("processCopy() ...");
        
        try
        {
            dataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(targetDataobjectTypeId).array());
            targetTableName = dataobjectType.getName();

            String gpMasterIP = getDsConnPropValue(targetDatasourceConnection, "ip_address");
            int gpMasterPort = 22;
            String gpMasterUser = "gpadmin";
            String gpMasterPassword = "gpadmin";
            String gpMasterPrivateKey = null;
            boolean gpMasterUsePassword = true;
            String gpTargetDB = getDsConnPropValue(targetDatasourceConnection, "database_name");
            String gpTargetDBPort = getDsConnPropValue(targetDatasourceConnection, "port");
            if (gpTargetDBPort.isEmpty()) gpTargetDBPort = DatabaseType.GREENPLUM.getDefaultPort();
            
            String cmdPSQL = String.format("psql -p %s -U %s -d %s -f %s/%s.sql",gpTargetDBPort, gpMasterUser, gpTargetDB, folderNameForMaster, databaseItem);
            Pair<String, Boolean> retPairValue = null;
            int retryCnt = 0;
            while (retryCnt < 3) {
                retPairValue = execShellScript(cmdPSQL, gpMasterIP, gpMasterPort, gpMasterUser, gpMasterPassword, gpMasterPrivateKey, gpMasterUsePassword);
                if(retPairValue.getValue() && !retPairValue.getKey().contains(" ERROR:")) {
                   break; 
                } else {
                    retryCnt ++ ;
                    Thread.sleep(3000);
                }
            }
            if (retryCnt == 3) {
                errorInfo.append(String.format("Error break in COPY: Failed when execute command(%s) in GP Master (IP=%s)!",cmdPSQL,gpMasterIP));
                errorInfo.append(retPairValue.getKey());
                return false;
            }
            else log.info(String.format("Execute command(%s) in GP Master (IP=%s) succeedfully! Result: %s",cmdPSQL,gpMasterIP,retPairValue.getKey()));

            String SQLCount = generateQuerySQLCount(DatabaseType.GREENPLUM, faSchema, DatabaseItemType.DB_TABLE.getValue(), targetTableName, null);
            targetDBConn = JDBCUtil.getJdbcConnection(targetDatasourceConnection);
            targetStatement = targetDBConn.prepareStatement(SQLCount);
            targetResultSet = JDBCUtil.executeQuery(targetStatement);
            targetResultSet.next();
            long targetCount = targetResultSet.getLong(1); 
            if (targetCount == totalCount) {
                log.info("Copy data to GP succeeded !!!! totalCount="+totalCount);

                String cmdRm = String.format("rm -Rf %s/%s*", folderNameForMaster, databaseItem);
                execShellScript(cmdRm, gpMasterIP, gpMasterPort, gpMasterUser, gpMasterPassword, gpMasterPrivateKey, gpMasterUsePassword);

                for (String filePut: fileList) new File(filePut).delete();
            } else {
                errorInfo.append(String.format("Error break in COPY: Failed when copy file(%s) data to GP table(%s)! Source row count (%d) != target row count (%d)",outputFolder,databaseItem,totalCount,targetCount));
                return false;
            }
        }
        catch (Exception e)
        {
            String errorStr = "processCopy() failed! e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            throw new Exception(errorStr);            
        }
        finally
        {
            JDBCUtil.close(targetResultSet);
            JDBCUtil.close(targetStatement);
            JDBCUtil.close(targetDBConn);
        }
        return true;
    }
    
    class DBExporter implements Runnable
    {
        private int no;
        StringBuilder errorInfo;
        private Writer controlFileWriter;
        private Writer writer;
        private Connection sourceDBConn = null;
     
        public DBExporter(int no, StringBuilder errorInfo) 
        {
            this.no = no;
            this.errorInfo = errorInfo;
        }       
    
        @Override
        public void run()
        {
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            String querySQL = null;
            int processSucceedCount;
            int page;
            String data;
                   
            int retry1 = 0;
            while(true)
            {
                childThreadStatus[no] = "waiting";
                try {
                    data = dataQueue.take(); // will block when no new message
                } catch (InterruptedException ex) {
                    String errorStr = "Threads-"+no+" process table "+tableName+" failed when take data from queue!"+ex;
                    log.error(errorStr);
                    if (retry1 > 3) {
                        errorInfo.append(errorStr);
                        childThreadStatus[no] = "error";
                        break;
                    }
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ex1) {}
                    retry1 ++ ;
                    continue;
                }
                
                childThreadStatus[no] = "processing";
                
                int retry = 0;
                while(true)
                {
                    retry ++;
                    
                    try
                    {
                        page = Integer.parseInt(data);
                        long startLine = (page-1)*PAGE_SIZE+1;
                        int pageSize = PAGE_SIZE;
                        
                        if ( sourceDBConn == null )
                            sourceDBConn = JDBCUtil.getJdbcConnection(datasourceConnection);
                        querySQL = generateQuerySQL(sourceDBConn,startLine,pageSize);

                        log.info("exportData() ... table = "+tableName+" page="+page+" querySQL=" + querySQL);

                        statement = sourceDBConn.prepareStatement(querySQL);
                        statement.setFetchSize(1000);

                        resultSet = JDBCUtil.executeQuery(statement);

                        if ( resultSet.next() == false )
                        {
                            log.info(" no more result!");
                            break;
                        }
                        else
                        {
                            String outputFileName = String.format("%s/%s_%04d.csv",outputFolder,tableName,page);
                            String shortOutputFileName = String.format("%s_%04d.csv",tableName,page);
                            fileList.add(outputFileName);

                            boolean append = true;
                            controlFileWriter = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(fullOutputControlFileName,append),"UTF-8"));
                            if ( skipErrorLine == null )
                                controlFileWriter.write(String.format("COPY %s FROM '%s/%s' DELIMITER '%s'; \n",targetTableName,folderNameForMaster,shortOutputFileName,delimiter));
                            else
                                controlFileWriter.write(String.format("COPY %s FROM '%s/%s' DELIMITER '%s' null '' segment reject limit %s; \n",targetTableName,folderNameForMaster,shortOutputFileName,delimiter,skipErrorLine));
                            controlFileWriter.close();

                            processSucceedCount = 0;
                            ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
                            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileName),"UTF-8"));
                            do
                            {
                                StringBuilder builder = new StringBuilder();
                                String value = "";

                                int colNum = resultSetMetadata.getColumnCount();
                                for (int k = 1; k <= colNum; k++) 
                                {
                                    String type = resultSetMetadata.getColumnTypeName(k);
                                    String columnName = resultSetMetadata.getColumnName(k);

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
                                        byte[] b = resultSet.getBytes(columnName);
                                        value = generateGreenPlumByteaString(b);
                                    } 
                                    else 
                                    if ( type.equals("CLOB") ) 
                                    {
                                        Clob b = resultSet.getClob(columnName);
                                        value = FileUtil.isNullToCharEmpty(new String(Base64.encodeBase64(JDBCUtil.getClobString(b)), "UTF-8"),delimiter);
                                    }
                                    else 
                                    if ( type.equals("VARCHAR") || type.equals("CHAR") || type.equals("VARCHAR2") ) 
                                    {
                                        value = FileUtil.isNullToCharEmpty(resultSet.getString(columnName),delimiter);
                                    } 
                                    else
                                        value = FileUtil.isNullToChar(resultSet.getString(columnName));

                                    if (k > 1)
                                        builder.append(delimiter);

                                    builder.append(value.replace(delimiter, "-"));
                                }

                                if ( addEmptyEdfIdField )
                                    builder.append(delimiter).append("\"\""); // for edf_id
                                
                                if ( addEdfLastModifyTimeField )
                                    builder.append(delimiter).append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())); // for edf_id EdfLastModifyTime

                                builder.append("\n");

                                writer.write(builder.toString());
                                processSucceedCount++;

                                if ( processSucceedCount%FLUSH_LINE == 0 )
                                    writer.flush();
                            }
                            while(resultSet.next());
                            
                            lock.lock();
                            totalCount += processSucceedCount;
                            lock.unlock();
                            
                            writer.close();
                            JDBCUtil.close(resultSet);
                            JDBCUtil.close(statement);
                        }       

                        childThreadStatus[no] = "done";
                        dataService.updateDatasourceEndJobItemNumber(job.getOrganizationId(), job.getId(), JobItemProcessingStatisticType.PROCESSED_ITEMS.getValue(), (int) totalCount);
                        log.info("exportData() success to file="+String.format("%s/%s_%04d.csv",outputFolder,tableName,page)+"! tableName="+tableName+" querySQL="+querySQL);
                        
                        break;
                    }
                    catch (Exception e)
                    {
                        String errorStr = "exportData() failed! tableName="+tableName+" querySQL="+querySQL+" e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e);
                        log.error(errorStr);  
                        try
                        {
                            if ( sourceDBConn != null )
                                sourceDBConn.close();
                        }
                        catch(Exception ee)
                        {
                        }

                        sourceDBConn = null;
                        
                        if ( retry > 3 )
                        {
                            errorInfo.append(errorStr);
                            errorStr = " process table "+tableName+" failed! retry="+retry+"!!!";
                            log.error(errorStr);
                            errorInfo.append(errorStr);
                            childThreadStatus[no] = "error";
                            break; 
                        }
                    }
                    finally
                    {
                        JDBCUtil.close(resultSet);
                        JDBCUtil.close(statement);
                        JDBCUtil.close(sourceDBConn);
                    }
                }
                if (childThreadStatus[no].equals("error")) break;
            }
        }
    }
    
    private Pair<String, Boolean> scpFile(String fileName, String targetDirectory, String actionType, String IP, int port, String user, String password, String privateKey, boolean usePassword) 
    {
        ch.ethz.ssh2.Connection connection = new ch.ethz.ssh2.Connection(IP, port);
        boolean isAuthed; 
        Boolean retValue;
        StringBuilder errorInfo = new StringBuilder();
        
        
        try {
            connection.connect();
            if (usePassword) {
		isAuthed = connection.authenticateWithPassword(user, password);
            } else {
                isAuthed = connection.authenticateWithPublicKey(user, new File(privateKey), password);
            }
            if (isAuthed) {
                SCPClient scpClient = connection.createSCPClient();
                if (actionType.equals("PUT")) {
                    scpClient.put(fileName, targetDirectory);
                } else {
                    scpClient.get(fileName, targetDirectory);
                }
                retValue = true;
            } else {
                log.error("scpFile()...认证失败!");
                errorInfo.append("scpFile()...认证失败!");
                retValue = false;
            }
        } catch (Exception e) {
            log.error("scpFile() failed!"+e);
            errorInfo.append(String.format("scpFile() failed! error=%s",e.getMessage()));
            retValue = false;
        } finally {
            connection.close();
        }
        return new Pair<>(errorInfo.toString(),retValue);
    }
    
    public Pair<String, Boolean> execShellScript(String cmd, String IP, int port, String user, String password, String privateKey, boolean usePassword)
    {  
        ch.ethz.ssh2.Connection connection = new ch.ethz.ssh2.Connection(IP, port);
        boolean isAuthed; 
        Session sess = null;  
        BufferedReader br = null;
        Boolean retValue;

        StringBuilder buffer = new StringBuilder("exec result:");  
        buffer.append(System.getProperty("line.separator"));// 换行  

        try {
            connection.connect();
            if (usePassword) {
								isAuthed = connection.authenticateWithPassword(user, password);
            } else {
                isAuthed = connection.authenticateWithPublicKey(user, new File(privateKey), password);
            }
            if (isAuthed) {
                sess = connection.openSession();  
                sess.execCommand(cmd);  

                br = new BufferedReader(new InputStreamReader(new StreamGobbler(sess.getStdout())));  
                while (true) {  
                    String line = br.readLine();  
                    if (line == null)  {
                        break;  
                    }
                    buffer.append(line);  
                    buffer.append(System.getProperty("line.separator"));// 换行  
                }  
                
                br = new BufferedReader(new InputStreamReader(new StreamGobbler(sess.getStderr())));  
                boolean isError = false;
                while (true) {  
                    String line = br.readLine();  
                    if (line == null)  {
                        break;  
                    }
                    isError = true;
                    buffer.append(line);  
                    buffer.append(System.getProperty("line.separator"));// 换行  
                }

                if(sess.getExitStatus() != null && sess.getExitStatus() != 0 || sess.getExitStatus() == null && isError) {
                    retValue = false;
                } else {
                    retValue = true;
                }
            } else {
                log.error("execShellScript()...认证失败!");
                retValue = false;
            }
        }
        catch (IOException e) {
            log.error("scpFile() failed!"+e);
            retValue = false;
        } 
        finally {
            if(br != null){
                try {
                    br.close();
                } catch (IOException ex) {
                    log.error("BufferedReader.close() failed!"+ex);
                }
            }
            if(sess != null){
                sess.close();  
            }
            connection.close();
        }
  
        return new Pair<>(buffer.toString(),retValue);  
    }  

    private String generateQuerySQLCount(DatabaseType databaseType, String schema, int databaseItemType, String tableName, String filter)
    {
        String sql;
        String schema1 = schema;
        String databaseItem = tableName;
        String databaseItem1 = tableName;
        
        if ( databaseItemType == DatabaseItemType.DB_VIEW.getValue() && databaseType == DatabaseType.GREENPLUM )
        {
            if ( !schema.isEmpty() )
                schema1 = String.format("\"%s\".",schema.substring(0, schema.length()-1));
            
            databaseItem1 = String.format("\"%s\"",databaseItem);
        }
        
        if ( filter == null || filter.trim().isEmpty() )
            sql = String.format("select count(*) from %s%s",schema1,databaseItem1);
        else
            sql = String.format("select count(*) from %s%s where %s",schema1,databaseItem1,filter);

        return sql;
    }

    private int getQueryCountOnly(Connection dbConn, String countQuerySQL) throws Exception
    {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try 
        {
            log.info(" countQuerySQL ="+countQuerySQL);
            
            statement = dbConn.prepareStatement(countQuerySQL);
            resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();
 
            return resultSet.getInt(1);             
        }
        catch(Exception e)
        {
            log.info("getQueryCount() failed! e="+e);
            throw e;
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(statement);
        }
    }
    
    private String getDsConnPropValue(DatasourceConnection datasourceConnection, String elementName) throws Exception
    {
        String valueStr = null;
        
        try
        {
            SAXReader saxReader = new SAXReader();
            ByteArrayInputStream in = new ByteArrayInputStream(datasourceConnection.getProperties().getBytes("utf-8"));
            Document propertiesXml= saxReader.read(in);

            List<Element> nodes = propertiesXml.selectNodes("//datasourceConnectionProperties/property");

            for( Element node: nodes)
            {
                if ( node.element("name").getTextTrim().equals(elementName) )
                {
                    valueStr = (String)node.element("value").getTextTrim();
                    break;
                }
            }
            
            if ( valueStr == null )  valueStr = "";
                        
            return valueStr;
        }
        catch(Exception e)
        {
            throw e;
        }
    }
    
    private String generateQuerySQL(Connection sourceDBConn,long startLine,int pageSize) // 1,1000; 1001,1000
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
        
        if ( filter == null || filter.trim().isEmpty() )
            querySql = String.format("select * from %s%s",schema1,databaseItem1);
        else
            querySql = String.format("select * from %s%s where %s",schema1,databaseItem1,filter);
        
        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            querySql = String.format("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM ( %s ) A WHERE ROWNUM <= %d ) WHERE RN >= %d",querySql,startLine+pageSize-1, startLine);
        }
        else
        if ( databaseType == DatabaseType.MYSQL )
        {
            querySql = String.format("%s limit %d,%d",querySql,startLine-1,pageSize);
        }   
        else
        if ( databaseType == DatabaseType.GREENPLUM )
        {
            String primaryKeyColumnNames = "";
            try {
                DatabaseMetaData databaseMetaData = sourceDBConn.getMetaData();
                ResultSet primaryKeyRs = databaseMetaData.getPrimaryKeys(null, null, databaseItem);
                while(primaryKeyRs.next()) 
                {
                    String primaryKeyColumnName = primaryKeyRs.getString("COLUMN_NAME");
                    primaryKeyColumnNames += String.format("%s,",primaryKeyColumnName);
                }
            } catch (SQLException ex) {
                log.info("generateQuerySQL() failed! e="+ex);
            }
            primaryKeyColumnNames = primaryKeyColumnNames.substring(0, primaryKeyColumnNames.length()-1);

            querySql = String.format("%s order by %s offset %d limit %d",querySql,primaryKeyColumnNames,startLine-1,pageSize);
        }  

        return querySql;
    }
    
    private String generateGreenPlumByteaString(byte[] b)
    {
        StringBuilder byteaString = new StringBuilder("");
        
        for(int i=0; i<b.length;i++) {
            int bi = b[i] & 0xFF;
            String byteString = String.format("\\\\%03d",Integer.parseInt(Integer.toOctalString(bi)));
            byteaString.append(byteString);
        }
        
        return byteaString.toString();
    }
    
}
