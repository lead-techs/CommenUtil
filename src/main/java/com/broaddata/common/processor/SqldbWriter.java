/*
 * SqldbRequestProcessor.java
 */

package com.broaddata.common.processor;

import com.broaddata.common.model.enumeration.MetadataDataType;
import org.apache.log4j.Logger;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import com.broaddata.common.util.JDBCUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import java.sql.Connection;

class SqldbWriter
{
    static final Logger log = Logger.getLogger("SqldbWriter");
    static final int BATCH_SIZE_PER_THREAD = 3000;
 
    public static Map<String,Object> objectCache = new HashMap<>();
      
    static final int SQL_THREAD_NUM = 6;
    private final BlockingQueue<List<DataobjectInfo>> dataQueue = new ArrayBlockingQueue<>(SQL_THREAD_NUM*100);
    volatile String[] childThreadStatus = new String[SQL_THREAD_NUM];
    private DBWriter[] childDBWriterList = new DBWriter[SQL_THREAD_NUM];
    
    private List<Map<String, Object>> metadataDefinitions;
    private String tableName;
    private DatasourceConnection datasourceConnection;
    private boolean needEdfIdField = true;
    private int organizationId;
    private DataobjectType dataobjectType;
    private int targetSqldbId;

    public SqldbWriter()
    {
        for(int i=0;i<SQL_THREAD_NUM;i++)
        {
            childThreadStatus[i] = "init";

            DBWriter dbWriter = new DBWriter(i);
            childDBWriterList[i] = dbWriter;

            Thread thread = new Thread(dbWriter);
            thread.start();
        }
    }
    
    public void setConfig(int organizationId, int targetSqldbId,String tableName,DataobjectType dataobjectType,List<Map<String, Object>> metadataDefinitions,DatasourceConnection datasourceConnection)
    {
        this.organizationId = organizationId;
        this.targetSqldbId = targetSqldbId;
        this.tableName = tableName;
        this.dataobjectType = dataobjectType;
        this.metadataDefinitions = metadataDefinitions;
        this.datasourceConnection = datasourceConnection;
        
        Map<String,Object> definition = new HashMap<>();
        
        definition.put("name","edf_last_modified_time");
        definition.put("description","最后修改时间");
        definition.put("dataType",String.valueOf(MetadataDataType.TIMESTAMP.getValue()));
        
        this.metadataDefinitions.add(definition);        
    }
        
    public void quitThread()
    {
        for(int i=0;i<SQL_THREAD_NUM;i++)
            childDBWriterList[i].quit();
    }
    
    public void processData( List<DataobjectInfo> doInfoList) throws Exception
    {
        Date startTime = new Date();
        int k = 0;
        
        try
        {
            log.info(" processData() ..... ");

            for(int i=0;i<SQL_THREAD_NUM;i++)
                childThreadStatus[i] = "init";
             
            List<DataobjectInfo> newData = new ArrayList<>();

            int jj = 0;

            for(DataobjectInfo doInfo : doInfoList)
            {                  
                newData.add(doInfo);

                jj++;

                if ( jj == BATCH_SIZE_PER_THREAD )
                {
                    dataQueue.put(newData);
                    newData = new ArrayList<>();
                                               
                    log.info(" jj = "+jj);
                    k++;

                    jj = 0;
                }
            }
                
            if ( jj > 0 )
            {
                dataQueue.put(newData);
                k++;
            }
            
            if ( k > SQL_THREAD_NUM )
                k = SQL_THREAD_NUM;

            Tool.SleepAWhileInMS(1000);

            int retry = 0;

            while(true)
            {
                retry++;

                int doneNum = 0;
                int failedNum = 0;

                for(int i=0;i<SQL_THREAD_NUM;i++)
                {
                    log.info("check thread: no="+i+" status="+childThreadStatus[i]+" retry="+retry+" dataQueue.size="+dataQueue.size());

                    if ( childThreadStatus[i].equals("done") )
                        doneNum++;
                    
                    if ( childThreadStatus[i].equals("failed") )
                        failedNum++;
                }

                if ( doneNum == k && dataQueue.isEmpty() )
                {
                    log.info("check thread: all done! doneNum="+doneNum);
                    break;
                }

                if ( failedNum > 0  )
                {
                   throw new Exception("sqldb writer processData failed! failedNum= "+k);
                }
                
                Tool.SleepAWhile(1, 0);
                
                if ( retry > 60*30 ) // 10 minutes
                    throw new Exception("sqldb writer processData failed! ");
            }
        }
        catch(Throwable e)
        {
            log.error("11111 processMessage() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }
    
    class DBWriter implements Runnable 
    {       
        private int no;
        private volatile boolean quit = false;

        private List<DataobjectInfo> sqldbData;
       
        public DBWriter(int no) 
        {
            this.no = no; 
        }
             
        public void quit()
        {
            quit = true;
        }
            
        @Override
        public void run()
        {      
            Connection conn = null;
            
            while(!quit)
            {
                try
                {              
                    if ( datasourceConnection == null )
                    {
                        log.info(" datasourceConnection is null");
                        Tool.SleepAWhile(1, 0);
                        continue;    
                    }
                    
                    if ( conn == null )
                    {
                        conn = JDBCUtil.getJdbcConnection(datasourceConnection);
                        conn.setAutoCommit(false);
                    }
                }
                catch(Exception e)
                {
                    log.error(" get jdbc conn failed! e="+e+" datasourceConnectionId="+datasourceConnection.getId());
                    Tool.SleepAWhile(1, 0);
                    continue;
                }
            
                try
                {
                    //log.info(" waiting message ...");
                    
                    sqldbData = dataQueue.poll();  // will block when no new message
                    
                    if ( sqldbData == null )
                    {
                        //log.info(" no data in dataQueue()");
                        Tool.SleepAWhileInMS(1000);
                        continue;
                    }
                                                                                
                    log.info(" took message and start to process!");

                    childThreadStatus[no] = "processing";
               
                    List<Map<String,String>> sqldbRows = new ArrayList<>();

                    for(DataobjectInfo doInfo : sqldbData)
                    {
                        List<FrontendMetadata> metadataList = doInfo.getMetadataList();
            
                        Map<String,String> sqldbRow = new HashMap<>();

                        sqldbRow.put("dataobjectId", doInfo.getDataobjectId());

                        for(int i=0;i<metadataList.size();i++)
                        {
                            if ( !metadataList.get(i).getName().toUpperCase().startsWith(dataobjectType.getName().toUpperCase()) )
                                continue;

                            String tableColumnName = Tool.changeTableColumnName(dataobjectType.getName().trim(),metadataList.get(i).getName(),true);            
                            sqldbRow.put(tableColumnName, metadataList.get(i).getSingleValue());
                        }
                        
                        sqldbRows.add(sqldbRow);
                    }              
                      
                    Date startTime1 = new Date();

                    int retry = 0;
                    
                    while(true)
                    {
                        retry++;
                        
                        try
                        {
                            //if ( dbConnectionKey == null )                              
                            //    dbConnectionKey = String.format("%s-%d",tableName,no);  
                        
                            Util.writeDataToSQLDBWithPreparedStatmentFromSQLDB(true,conn,metadataDefinitions,tableName,datasourceConnection,
                                    organizationId,dataobjectType,sqldbRows,false,null);
                            break;
                        }
                        catch(Exception e)
                        {
                            log.error(" Util.writeDataToSQLDB() failed! retry="+retry+" e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                            Tool.SleepAWhile(1,0);
                            
                            if ( retry > 3 )
                                throw e;
                        }
                    }
                    
                    log.info("333333 write to sqldb "+BATCH_SIZE_PER_THREAD+", took "+(new Date().getTime()-startTime1.getTime())+"ms");
                                                            
                    childThreadStatus[no] = "done";
                }
                catch(Exception e)
                {
                    log.error(" DBWrite failed! e="+e);
                    childThreadStatus[no] = "failed";  
                    
                    JDBCUtil.close(conn);
                    conn = null;
                    
                    break;
                }
            }
            
            JDBCUtil.close(conn);
            
            log.info("111111111111177777 quit DBWriter ! no="+no+" close conn!!!");
        }    
    }
}
    