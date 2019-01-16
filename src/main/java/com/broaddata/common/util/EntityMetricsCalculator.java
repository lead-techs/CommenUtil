
package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import java.util.HashMap;
import java.util.Date;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.thrift.TException;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
 
import com.broaddata.common.model.organization.MetricsCalculationTask;
import com.broaddata.common.model.platform.EntityType;
import com.broaddata.common.model.platform.MetricsDefinition;
import com.broaddata.common.pojo.MetricsCalculationConfig;
 
import com.broaddata.common.thrift.dataservice.Dataset;
import com.broaddata.common.model.enumeration.DataviewType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.MetricsCaculationMethodType;
import com.broaddata.common.model.enumeration.ScriptType;
import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.AnalyticQuery;
import com.broaddata.common.model.organization.DatasourceConnection;
 
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.TagDefinition;
   
public class EntityMetricsCalculator implements Serializable  // calucate all metrics of one entityId , no child entity
{
    static final Logger log = Logger.getLogger("EntityMetricsCalculationManager");
            
    private int FETCH_SIZE = 100;  // 每次取 的笔数
    private int KEEP_LIVE_SECONDS = 60; // 数据在服务端保持的秒数
            
    private MetricsCalculationTask task;   
    private EntityType entityType;
    private String entityKey;
    private List<String> metricsIds;
    private Map<String,Double> metricsTagValues; // datetimestr, metricsid, metricsValue
    private Map<String,Double> mtValues; // name, value no time
    private EntityManager em;
    private EntityManager platformEm;
    private MetricsCalculationConfig config = new MetricsCalculationConfig();
    private Map<String,String> entityColumnValues = new HashMap<>();
    private Map<String,Dataset> dataviewResultMap = new HashMap<>();
    private DatasourceConnection datasourceConnection;
    private Connection dbConn;
    private Statement stmt;
    private String getEntityValuesSqlStr;
    private List<String> primaryKeyColumnNames; 
    private List<String> columnInfoList;
    private String dataobjectTypeName;
    private Repository repository;
       
    private static ScriptEngineManager factory = new ScriptEngineManager();
    private ScriptEngine engine;
    private List<MetricsDefinition> metricsDefinitionList;
    public Map<String,Object> objectCache = new HashMap<>();
    private Date businessTime;
    
    public EntityMetricsCalculator(Date businessTime,Repository repository,MetricsCalculationTask task,EntityType entityType, EntityManager em, EntityManager platformEm,DatasourceConnection datasourceConnection,Connection dbConn,String getEntityValuesSqlStr,List<String> primaryKeyColumnNames,List<String> columnInfoList,String dataobjectTypeName) {
        this.task = task;
        this.entityType = entityType;
        this.em = em;
        this.platformEm = platformEm;
        this.dbConn = dbConn;
        this.getEntityValuesSqlStr = getEntityValuesSqlStr;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
        this.columnInfoList = columnInfoList;
        this.dataobjectTypeName = dataobjectTypeName;
        this.repository = repository;
        this.businessTime = businessTime;
        this.datasourceConnection = datasourceConnection;
    }
    
    public void init() throws Exception
    {
        String sql;
     
        try
        {
            if ( task.getMetricsDefinitionIds() == null || task.getMetricsDefinitionIds().trim().isEmpty() )
            {
                throw new Exception(" no metrics definition selected!");
                //sql = String.format("from MetricsDefinition where organizationId=%d and entityType=%d and isEnabled=1 order by calculationOrder",task.getOrganizationId(),task.getEntityType());
            }
            else
                sql = String.format("from MetricsDefinition where organizationId=%d and entityType=%d and id in ( %s ) and isEnabled=1 order by calculationOrder",task.getOrganizationId(),task.getEntityType(),task.getMetricsDefinitionIds());

            log.info(" sql ="+sql);

            metricsDefinitionList = platformEm.createQuery(sql).getResultList();   

            if ( dbConn != null )
                stmt = dbConn.createStatement();
        }
        catch(Exception e)
        {
            log.error(" init() failed! e="+e);
            throw e;
        }
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
    
     public void close()
    {
        JDBCUtil.close(stmt);
    }
        
    public Map<String,Double> calculate() throws Exception
    {                    
        metricsTagValues = new HashMap<>();
        mtValues = new HashMap<>();
            
        calculateMetrics();
                                     
        return metricsTagValues;
    }
    
    public void calculateMetrics() throws Exception
    {
        String sql;

        try
        {
           // if ( entityType.getIsDataFromSqldb() == 1 ) // from sqldb
            entityColumnValues = getEntityColumnValuesFromSqldb();
           // else
           //     entityColumnValues = Util.getEntityColumnValues(entityId, em, platformEm, task.getOrganizationId(), entityType.getDataobjectType(), task.getRepositoryId());
 
            for (MetricsDefinition metricsDefinition : metricsDefinitionList)
            {
                if ( metricsDefinition.getCalculationConfig() == null || metricsDefinition.getCalculationConfig().trim().isEmpty() )
                    continue;
                
                config.loadFromCalculationConfigStr(metricsDefinition.getCalculationConfig());
                MetricsCaculationMethodType type = MetricsCaculationMethodType.findByValue(config.getCalculationMethodTypeId());
                
                switch(type)
                {
                   case FROM_DATAVIEW:
                       getMetricsValuesFromDataview(metricsDefinition);
                       break;
                   case FROM_SCRIPT:
                       getMetricsValuesFromScript(metricsDefinition);
                       break;
                   case FROM_PROGRAM:
                       getMetricsValuesFromProgram(metricsDefinition,repository);
                       break;

                   default:
                }
            }
        }
        catch(Exception e)
        {
            log.error(" EntityMetricsCalculator.calculateMetrics() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }
        
    private Map<String,String> getEntityColumnValuesFromSqldb() throws SQLException
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
    
    private void getMetricsValuesFromDataview(MetricsDefinition metricsDefinition) throws TException, Exception
    {
        AnalyticDataview dataview;
        String dataviewSql;
        String queryFilterStr;
        Map<String,String> filterParameter = new HashMap<>();
        Map<String,String> dataviewParameters = new HashMap<>();
        double dValue;
        String value;
        String newStr = "";
        String inputStr = "";                
        int retry = 0;
        Dataset dataviewDataset;
        
        Date startTime = Util.getStartTimeByTimeUnit(businessTime,metricsDefinition.getTimeUnit());
    
        while(true)
        {
            retry++;
            
            try
            {
                String key = String.format("dataview-%d",config.getDataviewId());
                dataview = (AnalyticDataview)objectCache.get(key);
                
                dataviewSql = (String)objectCache.get(key+"dataviewSql");
                
                if ( dataview == null )
                {
                    dataview = em.find(AnalyticDataview.class,config.getDataviewId());
                    objectCache.put(key, dataview);
                    
                    dataviewSql = new String(dataview.getSqlStatement());
                    objectCache.put(key+"dataviewSql",dataviewSql);
                }
                
                dataview.setSqlStatement(dataviewSql);
                              
                if ( dataview.getType() == DataviewType.NORMAL.getValue() || dataview.getType() == DataviewType.AGGREGATION.getValue() )
                {
                    key = String.format("query-%d",dataview.getId());
                    
                    queryFilterStr = (String)objectCache.get(key+"filter");
                    
                    if ( queryFilterStr == null )
                    {
                        queryFilterStr = new String(dataview.getFilterString());
                        objectCache.put(key+"filter",queryFilterStr);
                    }
                    
                    dataview.setFilterString(queryFilterStr);

                    if ( dataview.getNeedInputParameters() == 1 )
                    {
                        for(Map<String,String> input : config.getInputParameters() )
                        {
                            String parameterName = input.get("parameterName");
                            parameterName = parameterName.substring(0, parameterName.indexOf("-"));

                            String columnSelect = input.get("columnSelect");

                            if ( columnSelect.equals("metrics_business_time") )
                            {
                                String startTimeStr = Tool.convertDateToTimestampStringForES(startTime);
                                Date endTime = Util.getEndTimeByTimeUnit(businessTime,metricsDefinition.getTimeUnit());
                                String endTimeStr = Tool.convertDateToTimestampStringForES(endTime);

                                String businessTimeStrForES = String.format("%s TO %s", startTimeStr,endTimeStr);
                                filterParameter.put(parameterName,businessTimeStrForES);
                            }
                            else
                            {
                                inputStr = entityColumnValues.get(columnSelect);

                                if ( inputStr == null || inputStr.trim().isEmpty() )
                                {
                                    metricsTagValues.put(metricsDefinition.getShortName()+"~"+startTime.getTime()+"~"+metricsDefinition.getTimeUnit(), (double)-999);
                                    mtValues.put(metricsDefinition.getShortName(), (double)-999);
                                    return;
                                }

                                filterParameter.put(parameterName, inputStr);
                            }
                        }
            
                        String newFilterStr = Util.replaceFilterString(dataview.getFilterString(),filterParameter).trim();
                        newStr = newFilterStr;

                        if ( newStr.contains("selectPredefineRelativeTime") )
                        {
                            dataviewParameters.put("currentTime",Tool.convertDateToString(businessTime,"yyyy-MM-dd HH:mm:ss"));
                        }
                        
                        if ( !newStr.isEmpty() )
                            dataviewParameters.put("newFilterStr",newStr);
                    }                  
                }
                else // for sql
                {
                    for(Map<String,String> input : config.getInputParameters() )
                    {
                        String parameterNameStr = input.get("parameterName");

                        String[] vals = parameterNameStr.split("\\-");
                        String parameterName;

                        if ( vals.length == 3 )
                            parameterName = String.format("%s~%s",vals[0],vals[2]);
                        else
                            parameterName = String.format("%s",vals[0]);

                        String columnSelect = input.get("columnSelect");

                        if ( columnSelect.equals("metrics_business_time") )
                        {
                            String businessTimeStrForSQL = Tool.convertDateToString(businessTime, "yyyy-MM-dd");
                            filterParameter.put(parameterName,businessTimeStrForSQL);
                        }
                        else
                        {
                            inputStr = entityColumnValues.get(columnSelect);

                            if ( inputStr == null || inputStr.trim().isEmpty() )
                            {
                                metricsTagValues.put(metricsDefinition.getShortName()+"~"+startTime.getTime()+"~"+metricsDefinition.getTimeUnit(), (double)-999);
                                mtValues.put(metricsDefinition.getShortName(), (double)-999);
                                return;
                            }

                            filterParameter.put(parameterName, inputStr);
                        }
                    }

                    String newSqlStr = Util.replaceFilterString(dataview.getSqlStatement(),filterParameter).trim();
                    newStr = newSqlStr;

                    if ( !newStr.isEmpty() )
                        dataviewParameters.put("newSqlStr",newStr);
                }             
                
                key = String.format("dataviewDataset-%d-%s",config.getDataviewId(),newStr);
                
                dataviewDataset = dataviewResultMap.get(key);
                
                if ( dataviewDataset == null )
                {
                   //dataset = SearchUtil.getDataviewDataWithScroll(organizationId,em,platformEm,dataviewBuf,dataviewId,parameters,fetchSize,keepLiveSeconds,needDatasetInfo);
                    dataviewDataset = SearchUtil.getDataviewData(task.getOrganizationId(),em,platformEm,null,config.getDataviewId(),dataviewParameters, FETCH_SIZE, KEEP_LIVE_SECONDS, false);
                    dataviewResultMap.put(key,dataviewDataset);
                }
                                             
                if ( dataviewDataset.getRows().isEmpty() )
                    dValue = 0.0;
                else
                {
                    value = dataviewDataset.getRows().get(config.getOutputRowNumber()-1).get(config.getOutputColumnName());
                    
                    if ( metricsDefinition.getCalculationScript() == null || metricsDefinition.getCalculationScript().trim().isEmpty() )
                        dValue = Tool.getDoubleValueFromString(value);
                    else
                        dValue = getValuesFromScript(config.getOutputColumnName(),value,metricsDefinition.getCalculationScript());
                }
               
                dValue = Tool.convertDouble(dValue, metricsDefinition.getDataPrecision(), BigDecimal.ROUND_HALF_UP);
                
                metricsTagValues.put(metricsDefinition.getShortName()+"~"+startTime.getTime()+"~"+metricsDefinition.getTimeUnit(), dValue);
                mtValues.put(metricsDefinition.getShortName(), dValue);
                
                break;
            }
            catch(Exception e)
            {              
                log.error("get dataset data failed!");
              
                if ( retry > 3)
                    throw e;
                
                Tool.SleepAWhile(3, 0);
            }
        }
    }
    
    private void getMetricsValuesFromScript(MetricsDefinition metricsDefinition) throws Exception
    {
        Double dValue = 0.0;
        
        Date startTime = Util.getStartTimeByTimeUnit(businessTime,metricsDefinition.getTimeUnit());
    
        engine = factory.getEngineByName(ScriptType.findByValue(config.getScriptTypeId()).name().toLowerCase());
         
        for(Map.Entry<String,Double> entry : mtValues.entrySet())
              engine.put(entry.getKey(),entry.getValue());
        
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
         
        try
        {
            Object obj = engine.eval(metricsDefinition.getCalculationScript());
            dValue = Double.valueOf(String.valueOf(obj));
     
            dValue = Tool.convertDouble(dValue, metricsDefinition.getDataPrecision(), BigDecimal.ROUND_HALF_UP);
        }
        catch(Exception e)
        {
            log.error(" engine eval failed! e="+e+" script ="+metricsDefinition.getCalculationScript());
            throw e;
        }
        
        metricsTagValues.put(metricsDefinition.getShortName()+"~"+startTime.getTime()+"~"+metricsDefinition.getTimeUnit(),dValue);    
        mtValues.put(metricsDefinition.getShortName(),dValue);
    }
    
    private double getValuesFromScript(String columnName,String value,String calculationScript) throws Exception
    {
        Double dValue = 0.0;
        
        try
        {
            engine = factory.getEngineByName(ScriptType.GROOVY.name().toLowerCase());
            
            columnName = Tool.convertColumnName(columnName);
            engine.put(columnName,value);
            
            Object obj = engine.eval(calculationScript);
            dValue = Double.parseDouble(String.valueOf(obj));
        }
        catch(Exception e)
        {
            log.error(" getValuesFromScript() faild! value="+value+" script="+calculationScript+" e="+e);
            throw e;
        }
        
        return dValue;
    }
    
    private void getTagValuesFromScript(TagDefinition tagDefinition,int timeUnit,ScriptEngine engine) throws Exception
    {
        Double dValue = 0.0;
        
        Date startTime = Util.getStartTimeByTimeUnit(businessTime,timeUnit);

        try
        {
            //dValue = Double.parseDouble(engine.eval(tagDefinition.getCalculationScript()).toString());       
        }
        catch(Exception e)
        {
            //log.error(" engine eval failed! e="+e+" script ="+tagDefinition.getCalculationScript());
            throw e;
        }
        
        metricsTagValues.put(tagDefinition.getShortName()+"~"+startTime.getTime()+"~"+timeUnit,dValue);    
        mtValues.put(tagDefinition.getShortName(),dValue);
    }
    
    private void getMetricsValuesFromProgram(MetricsDefinition metricsDefinition,Repository repository) throws Exception
    {
        double dValue = 0.0;
        
        Date startTime = Util.getStartTimeByTimeUnit(businessTime,metricsDefinition.getTimeUnit());
    
        String className = config.getProgramClass();
        
        try
        {
            ProgramMetricsCalculationBase programMetricsCalculator = Util.getProgramMetricsCalculationClass(className,this);
            
            Map<String,Object> parameters = new HashMap<>();
            
            for(String para : programMetricsCalculator.PARAMETERS)
            {
                for(Map<String,String> map : config.getInputParameters())
                {
                    if ( map.get("parameterName").equals(para) )
                    {
                        parameters.put(para, map.get("columnSelect"));
                        break;
                    }
                }
            }
            
            parameters.put("repository", repository);

            dValue = programMetricsCalculator.calculate(entityType,entityKey,metricsDefinition,parameters);
            
            dValue = Tool.convertDouble(dValue, metricsDefinition.getDataPrecision(), BigDecimal.ROUND_HALF_UP);
        }
        catch(Exception e)
        {
            log.error("getMetricsValuesFromProgram(() failed! e="+e+"ClassName="+className);
            throw e;
        }
        
        metricsTagValues.put(metricsDefinition.getShortName()+"~"+startTime.getTime()+"~"+metricsDefinition.getTimeUnit(),dValue);    
        mtValues.put(metricsDefinition.getShortName(),dValue);
    }
     
    public MetricsCalculationTask getTask() {
        return task;
    }

    public void setTask(MetricsCalculationTask task) {
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

    public Map<String, Double> getMetricsValue() {
        return metricsTagValues;
    }

    public void setMetricsValue(Map<String, Double> metricsValues) {
        this.metricsTagValues = metricsValues;
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

    public Date getBusinessTime() {
        return businessTime;
    }

    public Connection getDbConn() {
        return dbConn;
    }

    public DatasourceConnection getDatasourceConnection() {
        return datasourceConnection;
    }

    public String getEntityKey() {
        return entityKey;
    }

    public void setEntityKey(String entityKey) {
        this.entityKey = entityKey;
    }
}
