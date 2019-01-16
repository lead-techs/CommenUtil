
package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.MetadataDataType;
import org.apache.log4j.Logger;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import java.util.HashMap;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.broaddata.common.model.organization.TagProcessingTask;
import com.broaddata.common.model.platform.EntityType;

import com.broaddata.common.model.enumeration.ScriptType;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.TagProcessingJob;
import com.broaddata.common.model.platform.ConstantParameter;
import com.broaddata.common.model.platform.TagDefinition;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
   
public class EntityTagProcessor implements Serializable  // calucate all metrics of one entityId , no child entity
{
    static final Logger log = Logger.getLogger("EntityTagProcessor");
            
    private int FETCH_SIZE = 100;  // 每次取 的笔数
    private int KEEP_LIVE_SECONDS = 60; // 数据在服务端保持的秒数
            
    private TagProcessingTask task;   
    private EntityType entityType;
    private String entityKey;
    private List<String> metricsIds;
    private Map<String,String> tagValues; // datetimestr, metricsid, metricsValue
    private EntityManager em;
    private EntityManager platformEm;
 
    private Map<String,String> entityColumnValues = new HashMap<>();
    private ScriptEngine engine;
    private List<TagDefinition> tagDefinitionList;
    private static ScriptEngineManager factory = new ScriptEngineManager();
        
    private DatasourceConnection datasourceConnection;
    private Connection dbConn;
    private Statement stmt;
    private String getEntityValuesSqlStr;
    private List<String> primaryKeyColumnNames; 
    private List<String> columnInfoList;
    private String dataobjectTypeName;
     
    public EntityTagProcessor(TagProcessingTask task, EntityType entityType, EntityManager em, EntityManager platformEm,DatasourceConnection datasourceConnection,Connection dbConn,String getEntityValuesSqlStr,List<String> primaryKeyColumnNames,List<String> columnInfoList,String dataobjectTypeName) {
        this.task = task;
        this.entityType = entityType;
        this.em = em;
        this.platformEm = platformEm;
        this.dbConn = dbConn;
        this.getEntityValuesSqlStr = getEntityValuesSqlStr;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
        this.columnInfoList = columnInfoList;
        this.dataobjectTypeName = dataobjectTypeName;
        this.datasourceConnection = datasourceConnection;
    }
     
    public void init() throws Exception
    {
        try
        {
            engine = factory.getEngineByName(ScriptType.findByValue(task.getScriptType()).name().toLowerCase());
     
            String sql = String.format("from ConstantParameter where organizationId=0 or organizationId=%d",task.getOrganizationId());
            List<ConstantParameter> list = platformEm.createQuery(sql).getResultList();
            
            for(ConstantParameter para : list)
                engine.put(para.getName(),para.getValue());
                                    
            tagDefinitionList = TaggingUtil.getTagProcessingTaskTagDefinitions(platformEm,task);
            
            if ( dbConn != null )
                stmt = dbConn.createStatement();
        }
        catch(Exception e)
        {
            log.error(" init() failed! e="+e+" script ="+task.getCalculationScript());
            throw e;
        }
    }
    
    public void close()
    {
        JDBCUtil.close(stmt);
    }
 
    private Map<String,String> getEntityColumnValuesFromSqldb() throws Exception
    {
        String primaryKeySqlStr = "";
        String[] primaryKeyValues = entityKey.split("\\.");
        Map<String,String> map = new HashMap<>();
        
        int i=0;
        
        for(String primaryKeyColumnName : primaryKeyColumnNames)
        {
            for(String columnInfo : columnInfoList)
            {
                if ( columnInfo.contains(primaryKeyColumnName) )
                {
                    String[] vals = columnInfo.split("\\-");
                    
                    if ( vals[2].equals(String.valueOf(MetadataDataType.STRING.getValue())) )
                        primaryKeySqlStr += String.format("%s='%s' ",primaryKeyColumnName,primaryKeyValues[i]);
                    else
                        primaryKeySqlStr += String.format("%s=%s ",primaryKeyColumnName,primaryKeyValues[i]);
                    
                    if ( i+1 < primaryKeyColumnNames.size() )
                        primaryKeySqlStr += " and ";
                    
                    i++;
                    break;
                }
            }
        }
        
        String querySql = String.format("%s where %s",getEntityValuesSqlStr,primaryKeySqlStr);
        ResultSet resultSet = stmt.executeQuery(querySql);

        resultSet.next();
        
        for(String columnInfo : columnInfoList)
        {
            String[] vals = columnInfo.split("\\-");
            String tableColumnName = Tool.changeTableColumnName(dataobjectTypeName,vals[0],true);
    
            map.put(vals[0], resultSet.getString(tableColumnName));
        }
        
        JDBCUtil.close(resultSet);
         
        return map;
    }
      
    public Map<String,Object> getPrimaryKeyValues()
    {
        Map<String,Object> map = new HashMap<>();
        
        for(String primaryKeyColumnName : primaryKeyColumnNames)
        {
            for(String columnInfo : columnInfoList)
            {
                if ( columnInfo.contains(primaryKeyColumnName) )
                {
                    String[] vals = columnInfo.split("\\-");
     
                    String tableColumnName = Tool.changeTableColumnName(dataobjectTypeName,vals[0],true);
                    
                    String columnValue = entityColumnValues.get(vals[0]);
                   
                    int dataType = Integer.parseInt(vals[2]);
                    
                    Object obj = Util.getValueObject(columnValue, dataType);
                                      
                    map.put(tableColumnName,obj);    
                                    
                    break;
                }
            }
        }
        
        return map;
    }
      
    public Map<String,String> calculateTag() throws Exception
    {
        boolean isMultipleChoice = false;
        Map<String,List<String>> tagValueMap = null;
        Map<String,String> tagValueMap1 = null;
        
        try
        {
            tagValues = new HashMap<>();
      
            entityColumnValues = getEntityColumnValuesFromSqldb();
      
            for( Map.Entry<String,String> entry : entityColumnValues.entrySet() )
            {
                String columnName = entry.getKey();
                
                for(String columnInfo : columnInfoList)
                {
                    if ( columnInfo.contains(columnName) )
                    {
                        String[] vals = columnInfo.split("\\-");
 
                        int dataType = Integer.parseInt(vals[2]);

                        Object obj = Util.getValueObject(entry.getValue(), dataType);
 
                        engine.put(columnName,obj);
                        break;
                    }
                }
            }
            
            Object result = engine.eval(task.getCalculationScript());    
            
            if (result instanceof Map<?,?>)
            {
                try
                {
                    tagValueMap  = (Map<String,List<String>>)result; 
                    List<String> tagValueList;
                    
                    for(Map.Entry<String,List<String>> entry : tagValueMap.entrySet())
                        tagValueList = entry.getValue();
                                        
                    isMultipleChoice = true;
                }
                catch(Exception e)
                {
                    tagValueMap1 = (Map<String,String>)result;
                    isMultipleChoice = false;
                }
                
                if ( isMultipleChoice )
                {
                    for(Map.Entry<String,List<String>> entry : tagValueMap.entrySet())
                    {
                        boolean found = false;

                        List<String> tagValueList = entry.getValue();
                        String vals = "";

                        for(String val : tagValueList)
                            vals = String.format("%s~%s",vals,val);

                        for(TagDefinition tagDefinition : tagDefinitionList)
                        {
                            if ( tagDefinition.getShortName().trim().equals(entry.getKey().trim()) )
                            {
                                String key = String.valueOf(tagDefinition.getId())+"-"+entry.getKey();

                                if ( tagDefinition.getIsMutipleChoices() == 0 && tagValueList.size() >1 )
                                {
                                    String errorInfo = "multiple choice is not allowed! tagdefinition name="+tagDefinition.getShortName();
                                    log.error(errorInfo);
                                    throw new Exception(errorInfo);
                                }                           

                                tagValues.put(key,vals);

                                found = true;
                                break;
                            }
                        }

                        if ( !found )
                        {
                            String errorInfo = "target tag not found in result! name="+entry.getKey();
                            log.error(errorInfo);
                            throw new Exception(errorInfo);
                        }
                    }
                }
                else // single 
                {
                    for(Map.Entry<String,String> entry : tagValueMap1.entrySet())
                    {
                        boolean found = false;

                        String tagValue = entry.getValue();
                    
                        for(TagDefinition tagDefinition : tagDefinitionList)
                        {
                            if ( tagDefinition.getShortName().trim().equals(entry.getKey().trim()) )
                            {
                                String key = String.valueOf(tagDefinition.getId())+"-"+entry.getKey();
                                tagValues.put(key,tagValue);

                                found = true;
                                break;
                            }
                        }

                        if ( !found )
                        {
                            String errorInfo = "target tag not found in result! name="+entry.getKey();
                            log.error(errorInfo);
                            throw new Exception(errorInfo);
                        }
                    }
                }   
            }
            else
            {
                String tagValue  = String.valueOf(result);
                tagValues.put(String.valueOf(tagDefinitionList.get(0).getId())+"-"+tagDefinitionList.get(0).getShortName(),tagValue);
            }
        }
        catch(Exception e)
        {
            log.error(" engine eval failed! e="+e+" script ="+task.getCalculationScript());
            throw e;
        }
        
        return tagValues;
    }
     
    public TagProcessingTask getTask() {
        return task;
    }

    public void setTask(TagProcessingTask task) {
        this.task = task;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }
 
    public List<String> getMetricsIds() {
        return metricsIds;
    }

    public void setMetricsIds(List<String> metricsIds) {
        this.metricsIds = metricsIds;
    }

    public Map<String, String> getTagValues() {
        return tagValues;
    }

    public void setMetricsValue(Map<String, String> tagValues) {
        this.tagValues = tagValues;
    }

    public EntityManager getEm() {
        return em;
    }

    public void setEm(EntityManager em) {
        this.em = em;
    }

    public EntityManager getPlatformEm() {
        return platformEm;
    }

    public void setPlatformEm(EntityManager platformEm) {
        this.platformEm = platformEm;
    }

    public Map<String, String> getEntityColumnValues() {
        return entityColumnValues;
    }

    public DatasourceConnection getDatasourceConnection() {
        return datasourceConnection;
    }

    public Connection getDbConn() {
        return dbConn;
    }

    public String getEntityKey() {
        return entityKey;
    }

    public void setEntityKey(String entityKey) {
        this.entityKey = entityKey;
    }
    
    
}
