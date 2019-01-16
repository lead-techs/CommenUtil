/*
 * MessageProcessor.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Iterator;
import org.json.JSONObject;
import org.apache.commons.lang.exception.ExceptionUtils;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.Element;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Statement;
import org.dom4j.io.SAXReader;

import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.KafkaMessage;
import com.broaddata.common.model.platform.DataobjectType;

public class MessageProcessor1
{      
    static final Logger log = Logger.getLogger("MessageProcessor");
    
    static final int MESSAGE_BATCH_SIZE = 5000;
    
    private Map<String,Object> ObjectCache = new HashMap<>();
       
    private int organizationId;
    private int repositoryId;
    private int sourceApplicationId;
    private String messageType;
    private String datasourceConnectionName;
    private List<String> skipedMessageNames;
    private int clientNodeId;
    private Connection conn = null;
    private Statement stmt = null;
    private Map<String,String> insertSqlCache;
    private Map<String,String> updateSqlCache;
       
    public MessageProcessor1(int organizationId, int repositoryId, int sourceApplicationId,String messageType,String datasourceConnectionName,List<String> skipedMessageNames,int clientNodeId)
    {
        this.organizationId = organizationId;
        this.repositoryId = repositoryId;
        this.sourceApplicationId = sourceApplicationId;
        this.messageType = messageType;
        this.datasourceConnectionName = datasourceConnectionName;
        this.clientNodeId = clientNodeId;
        this.skipedMessageNames = skipedMessageNames;
    }
    
    public void setSqlCache(Map<String,String> insertSqlCache,Map<String,String> updateSqlCache)
    {
        this.insertSqlCache = insertSqlCache;
        this.updateSqlCache = updateSqlCache;
    }
    
    public String processMessages(DataService.Client dataService,long totalNumber,String topic,String message,Map<String,String> processingInfoMap) throws Exception
    { 
        String childMessage;
        List<Map<String, Object>> results;
        String originalTableName;
        String newTableName;
        String errorInfo;
        
        boolean needEdfIdField = true;
        DatasourceConnection datasourceConnection = null;
        DatasourceConnection repositoryDatasourceConnection = null;
        Datasource datasource = null;
        int dataobjectTypeId;
        DataobjectType dataobjectType = null;
        List<Map<String,Object>> metadataDefinitions;
        String opType;
        int kk = 0;

        Map<String,String> sqlMap;
                         
        try
        {
            message = message.trim();
            String currentMessageType = "ogg";
 
            if ( currentMessageType.equals(messageType) )
            {
                try
                {
                    repositoryDatasourceConnection = (DatasourceConnection)ObjectCache.get(String.format("repositoryDatasourceConnection-%d",repositoryId));
                    //log.info(" repositoryDatasourceConnection = "+repositoryDatasourceConnection);
                    if ( repositoryDatasourceConnection == null )
                    {
                        log.info(" get repisotry datasource connection");
                        repositoryDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getRepositorySQLdbDatasourceConnection(organizationId,repositoryId).array()); 
                        ObjectCache.put(String.format("repositoryDatasourceConnection-%d",repositoryId),repositoryDatasourceConnection);
                    }                    

                    datasourceConnection = (DatasourceConnection)ObjectCache.get(String.format("datasourceConnection-%s",datasourceConnectionName));
                    //log.info(" datasourceConnection = "+datasourceConnection);
                    if ( datasourceConnection == null )
                    {
                        log.info(" get datasource connection ");
                        datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(organizationId,datasourceConnectionName).array());
                        ObjectCache.put(String.format("datasourceConnection-%s",datasourceConnectionName),datasourceConnection);
                    }    
                }
                catch(Exception e)
                {
                    errorInfo = " get datasourceConnection failed! e="+e+" databaseName="+datasourceConnectionName+" not in datasourceConnection!"+" stacktrace="+ExceptionUtils.getStackTrace(e);     
                    log.error(errorInfo);
                
                    throw new Exception(errorInfo);
                }

                int i = 0;
                int j = 0;

                while( j>=0 )
                {
                    j = message.indexOf("{\"table\":\"", i+1);

                    if ( j >= 0 )
                        childMessage = message.substring(i, j);
                    else
                        childMessage = message.substring(i);

                    i=j;

                    kk++;
                    log.info(" No="+kk+", childMessage = "+childMessage.substring(0, childMessage.indexOf("pos"))+" ...");
                    //log.info(" No="+kk+", childMessage = ["+childMessage+"]");
                    //log.debug("processing childMessage no="+kk);

                    JSONObject jsonObject = new JSONObject(childMessage);

                    originalTableName = jsonObject.optString("table");
                    originalTableName = originalTableName.substring(originalTableName.indexOf(".")+1); 

                    processingInfoMap.put("lastProcessedMessage",childMessage);

                    boolean found = false;

                    for(String skipedMessageName : skipedMessageNames)
                    {
                        if ( skipedMessageName.trim().toLowerCase().equals(originalTableName.trim().toLowerCase()) )
                        {
                            found = true;
                            break;
                        }
                    }

                    if ( found )
                    {
                        log.warn(" skip table: "+originalTableName);
                        continue;
                    }

                    opType = jsonObject.optString("op_type");

                    if ( opType == null )
                        opType = "";

                    if ( opType.toUpperCase().equals("D") )
                    {
                        log.info(" skip delete message");
                        continue;
                    }

                    JSONObject columnObject = jsonObject.optJSONObject("after");
                    Iterator it = columnObject.keys();

                    Map<String,Object> columnValues = new HashMap<>();

                    while (it.hasNext()) 
                    {  
                        String columnName = (String)it.next();  
                        String columnValue = columnObject.optString(columnName);

                        if ( columnValue == null || columnValue.equals("null") )
                            columnValue = "";

                        columnValues.put(columnName, columnValue); 
                    }

                    Map<String, Object> result = new HashMap<>();
                    result.put("fields",columnValues);
                    result.put("opType",opType);
                    result.put("id","0000000000000000000000000000000000000000");
 
                    Map<String,Object> config = (Map<String,Object>)ObjectCache.get(originalTableName);

                    if ( config == null )
                    {
                        config = new HashMap<>();

                        int h = 0;

                        while(true)
                        {
                            h++;

                            try
                            {
                                datasource = (Datasource)Tool.deserializeObject(dataService.getDatasourceByName(organizationId,originalTableName.toUpperCase()).array());
                                
                                log.info(" get datasource! table="+originalTableName);
                                
                                break;
                            }
                            catch(Exception e)
                            {
                                errorInfo =" get datasource failed! table name ["+originalTableName+"] not in datasource! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
                                log.error(errorInfo);

                                if ( h <= 3 )
                                {
                                    Tool.SleepAWhile(3, 0);
                                    continue;
                                }

                                KafkaMessage kafkaMessage = new KafkaMessage();
                                kafkaMessage.setDatabaseName(datasourceConnectionName);
                                kafkaMessage.setTableName(originalTableName);
                                kafkaMessage.setDataobjectTypeId(0);
                                kafkaMessage.setDatasourceId(0);
                                kafkaMessage.setMessageBody(childMessage);
                                String messageKey = DataIdentifier.generateHash(childMessage);
                                kafkaMessage.setMessageKey(messageKey);
                                kafkaMessage.setOrganizationId(organizationId);
                                kafkaMessage.setRepositoryId(repositoryId);
                                kafkaMessage.setDatasourceConnectionId(datasourceConnection.getId());
                                kafkaMessage.setStatusTime(new Date());
                                kafkaMessage.setProcessInfo(errorInfo);
                                kafkaMessage.setProcessingNodeId(clientNodeId);

                                dataService.saveErrorKafkaMessage(organizationId,ByteBuffer.wrap(Tool.serializeObject(kafkaMessage)));

                                Tool.SleepAWhile(30, 0);

                                throw new Exception(errorInfo);
                            }
                        }

                        getDatasourceConfig(datasource,config);     

                        dataobjectTypeId = (Integer)config.get("dataobjectTypeId");
                        dataobjectType = (DataobjectType)Tool.deserializeObject(dataService.getDataobjectType(dataobjectTypeId).array());

                        config.put("datasource",datasource);
                        config.put("dataobjectType",dataobjectType);

                        newTableName = dataService.getDataobjectTypeTableName(organizationId,dataobjectTypeId,repositoryId);
                        config.put("newTableName",newTableName);

                        metadataDefinitions = (List<Map<String,Object>>)Tool.deserializeObject(dataService.getDataobjectTypeMetadataDefinition(dataobjectTypeId,false,true).array());
                        config.put("metadataDefinitions",metadataDefinitions);

                        ObjectCache.put(originalTableName,config);
                    }
                    else
                    {
                        datasource = (Datasource)config.get("datasource");
                        dataobjectTypeId = (Integer)config.get("dataobjectTypeId");
                        dataobjectType = (DataobjectType)config.get("dataobjectType");     
                        newTableName = (String)config.get("newTableName");
                        metadataDefinitions = (List<Map<String,Object>>)config.get("metadataDefinitions");
                    }

                    int k = 0;

                    while(true)
                    {
                        k++;

                        try
                        {
                            sqlMap = Util.generateSql(metadataDefinitions,newTableName,repositoryDatasourceConnection,needEdfIdField,organizationId,repositoryId,dataobjectType,result,false,"yyyy-MM-dd","yyyy-MM-dd:HH:mm:ss",false);
                
                            Date date1 = new Date();
                            
                            for(Map.Entry<String,String> map : sqlMap.entrySet() )
                            {
                                String sqlKey = map.getKey();
                                
                                if ( sqlKey.startsWith("insert") )
                                {
                                    insertSqlCache.put(map.getKey(), map.getValue());
                                }
                                else
                                {
                                    if ( updateSqlCache.get(map.getKey()) != null )
                                    {
                                        log.info(" duplicate sql! key=["+map.getKey()+"] ");
                                        //log.info(" duplicate sql! ");
                                    }
                         
                                    updateSqlCache.put(map.getKey(), map.getValue());
                                }                                
                            }
                 
                            break;
                        }
                        catch(Exception e)
                        {
                            errorInfo = " generatesql failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
                            log.error(errorInfo);
 
                            if ( k > 3 )
                            {                          
                                KafkaMessage kafkaMessage = new KafkaMessage();
                                kafkaMessage.setDatabaseName(datasourceConnectionName);
                                kafkaMessage.setTableName(originalTableName);
                                kafkaMessage.setDataobjectTypeId(dataobjectTypeId);
                                kafkaMessage.setDatasourceId(datasource.getId());
                                kafkaMessage.setMessageBody(childMessage);
                                String messageKey = DataIdentifier.generateHash(childMessage);
                                kafkaMessage.setMessageKey(messageKey);
                                kafkaMessage.setOrganizationId(organizationId);
                                kafkaMessage.setRepositoryId(repositoryId);
                                kafkaMessage.setDatasourceConnectionId(datasourceConnection.getId());
                                kafkaMessage.setStatusTime(new Date());
                                kafkaMessage.setProcessInfo(errorInfo);
                                kafkaMessage.setProcessingNodeId(clientNodeId);

                                dataService.saveErrorKafkaMessage(organizationId,ByteBuffer.wrap(Tool.serializeObject(kafkaMessage)));

                                Tool.SleepAWhile(30, 0);

                                throw e;
                            }
                        }
                    }
                }
            }
            else
            {
                log.info("unknow messageType ="+currentMessageType+" messageType="+messageType);
            }

            return "";
        }
        catch(Exception e)
        {
            errorInfo = " process failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
                
            return errorInfo;
        }
    }
        
    public void commitDB(long currentNo,Map<String,String> processingInfoMap) throws Exception
    {
        Date startTime = new Date();

        try
        {
            log.info(" start to execute batch..");

            try {
                int[] ret = stmt.executeBatch();
            }
            catch(Exception e)
            {
                String errorInfo = " excute batch failed!  e=["+e+"] cause=["+e.getCause()+"]";
                log.error(errorInfo);
                processingInfoMap.put("processingInfo", errorInfo);
                throw e;
            }
            
            //log.info(" remove duplicate! sqlCache size="+insertSqlCache.size()+" batch size="+CommonKeys.KAFKA_MESSAGE_COMMIT_SIZE);
                                 
            insertSqlCache.clear();

            log.info(String.format("db execute batch, took [%d] ms, k=%d",new Date().getTime()-startTime.getTime(),currentNo) );
                 
            log.info(" start to commit..");

            conn.commit();
            log.info(String.format("db commiting, took [%d] ms, k=%d",new Date().getTime()-startTime.getTime(),currentNo) );
        }
        catch(Exception e)
        {
            String errorInfo = "99999 commit to database failed! e="+e;
            log.info(errorInfo);
            throw e;
        }
    }
    
 /*  public DataobjectInfo generateDataobjectFromOGGMessage(int organizationId,int repositoryId,int sourceApplicationId,String message,String messageParameters) throws Exception
    {
        DataobjectInfo dataobject = null;
        String dataobjectId;
        int datasourceTypeId;
        int dataobjectTypeId;
        String primaryKeyStr;
        String primaryKeyValue;
        FrontendMetadata metadata;
        List<FrontendMetadata> metadataList = new ArrayList<>();
        Datasource datasource = null;
        DatasourceConnection datasourceConnection = null;
        String tableName;
        String opType;
        Map<String,String> columnValues = new HashMap<>();
        String columnNameListStr = "";
        List<Map<String,Object>> databaseItemDefinitions;
        boolean isPartialUpdate;    
        String errorInfo;
        
        try
        {
            String databaseName=messageParameters;
            
            JSONObject jsonObject = new JSONObject(message);
 
            tableName = jsonObject.optString("table");
            tableName = tableName.substring(tableName.indexOf(".")+1);  log.info("1111111111 table name="+tableName+" databaseName="+databaseName);
                        
            opType = jsonObject.optString("op_type");
             
            if ( opType.toUpperCase().equals("D") )
                return null;
                
            if ( opType.toUpperCase().equals("U") )
                isPartialUpdate = true;
            else
                isPartialUpdate = false;
            
            JSONObject columnObject = jsonObject.optJSONObject("after");               
            Iterator it = columnObject.keys();

            while (it.hasNext()) 
            {  
                String columnName = (String)it.next();  
                String columnValue = columnObject.optString(columnName);  
                log.info("666666 columnName="+columnName+" value="+columnValue);

                if ( columnValue == null || columnValue.equals("null") )
                    columnValue = "";
                
                columnValues.put(columnName, columnValue);  
                columnNameListStr += columnName +",";
            }
            
            columnNameListStr = columnNameListStr.substring(0, columnNameListStr.length()-1); log.info(" columNanmeList="+columnNameListStr);
            
            Map<String,Object> config = (Map<String,Object>)ObjectCache.get(tableName);
            
            if ( config == null )
            {
                config = new HashMap<>();
                
                try
                {
                    datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(organizationId,databaseName).array());            
                }
                catch(Exception e)
                {
                    errorInfo = " get datasourceConnection failed! e="+e+" databaseName="+databaseName+" not in datasourceConnection!";     
                    log.error(errorInfo);
                    throw new Exception(errorInfo);
                }
                
                try
                {        
                    datasource = (Datasource)Tool.deserializeObject(dataService.getDatasourceByName(organizationId,tableName).array());
                }
                catch(Exception e)
                {
                    errorInfo=" get datasource failed! e="+e+" table name ["+tableName+"] not in datasource!";
                    log.error(errorInfo);
                    throw new Exception(errorInfo);
                }
                
                datasourceTypeId = datasource.getDatasourceType();
                
                config.put("datasourceConnection",datasourceConnection);
                config.put("datasource",datasource);
                config.put("datasourceTypeId",datasourceTypeId);
 
                getDatasourceConfig(datasource,config);     
                
                ObjectCache.put(tableName,config);
            }
            else
            {
                datasourceConnection = (DatasourceConnection)config.get("datasourceConnection");
                datasource = (Datasource)config.get("datasource");
                datasourceTypeId = (Integer)config.get("datasourceTypeId");                
            }
 
            dataobjectTypeId = (Integer)config.get("dataobjectTypeId");
            primaryKeyStr = (String)config.get("primaryKeyStr");                
            databaseItemDefinitions = (List<Map<String,Object>>)config.get("databaseItemDefinitions");
           
            if ( !hasFullPrimaryKeyValues(primaryKeyStr,columnValues) )
            {
                errorInfo = "not has full primrykey values !!! primaryKey = ("+primaryKeyStr+")";
                log.error(errorInfo);
                throw new Exception(errorInfo);
            }
                        
            primaryKeyValue = getPrimaryKeyValue(primaryKeyStr,columnValues,columnNameListStr);
            
            log.info("777777777777 primaryKeyStr="+primaryKeyStr+" primaryKeyValue="+primaryKeyValue);
            
            dataobjectId = DataIdentifier.generateDataobjectId(organizationId,sourceApplicationId,datasourceTypeId,datasource.getId(),primaryKeyValue,repositoryId);

            dataobject = new DataobjectInfo();
            dataobject.setDataobjectId(dataobjectId);
            dataobject.setSourceApplicationId(sourceApplicationId);
            dataobject.setDatasourceType(datasourceTypeId);
            dataobject.setDatasourceId(datasource.getId());
            dataobject.setDataobjectType(dataobjectTypeId);
            dataobject.setSize(0);       
            dataobject.setName(primaryKeyValue);
            dataobject.setSourcePath(String.format("%d/%s/%s",datasourceConnection.getId(),tableName,primaryKeyValue));
            dataobject.setIsPartialUpdate(isPartialUpdate);
            dataobject.setNeedToSaveImmediately(true);
            
            for(Map.Entry<String,String> entry : columnValues.entrySet())
            {
                String columnName = entry.getKey().toLowerCase(); 
                String value = entry.getValue().trim();
                log.info("55555 columnName =["+columnName+"] value=["+value+"]");
                               
                String metadataName = "";
                
                for( Map<String,Object> map : databaseItemDefinitions )
                {
                    String fieldName = (String)map.get("name");
                    int dataType = Integer.parseInt((String)map.get("dataType"));
  
                    log.debug("33333333333333333 columnName ="+columnName+"fieldName="+fieldName+"    dataType="+dataType);
                    
                    if ( columnName.equals(fieldName.trim().toLowerCase()) )
                    {
                        metadataName = (String)map.get("targetDataobjectMetadataName");
                        
                        log.info("1111 metadataName="+metadataName+"  value="+value+"    dataType="+dataType);
                        
                        if ( dataType == MetadataDataType.TIMESTAMP.getValue() )
                        {
                            Date date = Tool.convertDateStringToDateAccordingToPattern(value,"yyyy-MM-dd:HH:mm:ss");
                            
                            if ( date == null )
                                value = "";
                            else
                                value = String.valueOf(date.getTime());
                            
                            log.info("change timestamp to 1111 metadataName="+metadataName+"  value="+value+" date="+date);
                        }
                        else
                        if ( dataType == MetadataDataType.DATE.getValue() )
                        {
                            Date date = Tool.convertDateStringToDateAccordingToPattern(value,"yyyy-MM-dd" );
                               
                            if ( date == null )
                                value = "";
                            else
                                value = String.valueOf(date.getTime());
                            log.info("change date to 1111 metadataName="+metadataName+"  value="+value+" date="+date);
                        }
                                                                 
                        metadata = new FrontendMetadata(metadataName,true);
                        metadata.setSingleValue(value);
                        metadataList.add(metadata);
                       
                        break;
                    }
                }
                                                
                if ( metadataName.isEmpty() )
                {
                    log.error(" metadataName not found!  field name=["+columnName+"]");
                    throw new Exception(" metadataName not found!  field name=["+columnName+"]");
                }
            }
        
            dataobject.setMetadataList(metadataList);           
 
            return dataobject;
        }
        catch(Exception e)
        {           
            log.error(" generateDataobject() failed!  e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }    */
    
    private void getDatasourceConfig(Datasource datasource, Map<String,Object> map) throws Exception
    {
        String databaseSelectionStr = null;
           
        SAXReader saxReader = new SAXReader();
        ByteArrayInputStream in = new ByteArrayInputStream(datasource.getProperties().getBytes("utf-8"));
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
     
        int dataobjectTypeId =  Integer.parseInt(element.element("targetDataobjectType").getTextTrim());
        map.put("dataobjectTypeId",dataobjectTypeId);
        
        String primaryKeyStr = element.element("primaryKeyStr").getTextTrim();
        map.put("primaryKeyStr",primaryKeyStr);
        
        List<Map<String,Object>> databaseItemDefinitions = new ArrayList<>();
        
        nodes = doc.selectNodes("//databaseSelection/fields/field");
            
        for( Element node : nodes)
        {
            Map<String,Object> definition = new HashMap<>();
            definition.put("name", node.element("name").getTextTrim());
            definition.put("dataType",node.element("dataType").getTextTrim());
            definition.put("isFileLink",node.element("isFileLink").getTextTrim().equals("true"));
            definition.put("backupDocument", node.element("backupDocument").getTextTrim().equals("true"));
            definition.put("useAsDataobjectName",node.element("useAsDataobjectName").getTextTrim().equals("true"));
            definition.put("targetDataobjectMetadataName",node.element("targetDataobjectMetadataName").getTextTrim());
            definition.put("targetDataobjectMetadataType",node.element("targetDataobjectMetadataType").getTextTrim());
            definition.put("datetimeFormat", node.element("datetimeFormat").getTextTrim());

            databaseItemDefinitions.add(definition);
        }
        
        map.put("databaseItemDefinitions",databaseItemDefinitions);
    }
    
    private boolean hasFullPrimaryKeyValues(String primaryKeyStr,Map<String,String> columnValues)
    {
        String [] columns;
        String value;
        
        if ( primaryKeyStr == null || primaryKeyStr.trim().isEmpty() ) 
            return true;
                
        columns = primaryKeyStr.split("\\.");

        for(String column:columns)
        {
            value = columnValues.get(column);
            if ( value == null || value.trim().equals("null") || value.trim().isEmpty() )
                return false;
        }

        return true;
    }
     
    private String getPrimaryKeyValue(String primaryKeyStr,Map<String,String> columnValues,String columnNameListStr)
    {
        String str = "";
        String [] columns;
        String value;
        
        if ( primaryKeyStr == null || primaryKeyStr.trim().isEmpty() )  // use all column as primary key
            columns = columnNameListStr.split("\\,");
        else
            columns = primaryKeyStr.split("\\.");

        for(String column:columns)
        {
            value = columnValues.get(column);
            if ( value == null || value.equals("null") )
                value = "";
  
            str += value +".";
        }

        str = str.substring(0, str.length()-1);
        
        return str;
    }
}


