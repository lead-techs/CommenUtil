/*
 * MetricsTagPreCalculationUtil.java
 */

package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.DatabaseType;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;

import com.broaddata.common.model.enumeration.SearchScope;
import com.broaddata.common.model.enumeration.SearchType;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.MetricsCalculationTask;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.organization.TagProcessingTask;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.EntityType;
import com.broaddata.common.model.platform.TagValue;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import com.broaddata.common.thrift.dataservice.SearchResponse;

public class MetricsTagPreCalculationUtil 
{
   static final Logger log = Logger.getLogger("MetricsTagPreCalculationUtil");
    
   public static List<Map<String,String>> preCalculateMetrics(EntityManager em,EntityManager platformEm,int entityTypeId, MetricsCalculationTask task, int numberOfTrialData,String computeTimeStr) throws Exception
    {
        List<Map<String,String>> list = new ArrayList<>();
        Map<String,String> map = new HashMap<>();
        String primaryKeyValue;
        Connection dbConn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        
        try
        {
            EntityType entityType = platformEm.find(EntityType.class, entityTypeId);
            Repository repository = em.find(Repository.class, task.getRepositoryId());
            DataobjectType dataobjectType = platformEm.find(DataobjectType.class,entityType.getDataobjectType());

           /* if ( entityType.getIsDataFromSqldb() == 0 ) // from es
            {
                int searchScope = dataobjectType.getIsEventData()==1?SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue():SearchScope.NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue();

                SearchRequest request = new SearchRequest(task.getOrganizationId(),task.getRepositoryId(),repository.getDefaultCatalogId(),SearchType.SEARCH_GET_ALL_RESULTS.getValue());

                request.setSearchScope(searchScope);

                List<String> indexTypes = Util.getAllTargetIndexType(platformEm,new Integer[]{dataobjectType.getId()});
                request.setTargetIndexTypes(indexTypes);
                request.setQueryStr("");

                 if ( entityType.getDataobjectFilter() == null || entityType.getDataobjectFilter().trim().isEmpty() )
                    request.setFilterStr("");
                else
                    request.setFilterStr(entityType.getDataobjectFilter());

                request.setColumns(String.format("%s;%s","_id",entityType.getDataobjectTypeNameFields()));

                SearchResponse response = SearchUtil.getDataobjectsWithScroll(em,platformEm,request,numberOfTrialData,CommonKeys.KEEP_LIVE_SECOND);
                List<Map<String,String>> searchResults = response.getSearchResults();
              
                String dataobjectTypeName = dataobjectType.getName();

                List<String> longPrimaryKeyFieldNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());

                List<String> primaryKeyColumnNames = new ArrayList<>();
                for(String longPrimaryKeyFieldName : longPrimaryKeyFieldNames)
                {
                    String tableColumnName = Tool.changeTableColumnName(dataobjectType.getName(),longPrimaryKeyFieldName,true);
                    primaryKeyColumnNames.add(tableColumnName);
                }

                List<String> columnInfoList = new ArrayList<>();
                Util.getSingleDataobjectTypeMetadataNames(columnInfoList,dataobjectType.getMetadatas(),true);
                
                Date computeTime = new Date();
                
                if ( computeTimeStr != null && !computeTimeStr.isEmpty() )
                {
                    computeTime = Tool.convertStringToDate(computeTimeStr, "yyyy-MM-dd HH:mm:ss");
                    
                    if ( computeTime == null )
                        computeTime = new Date();
                }
                 
                EntityMetricsCalculator calculator = new EntityMetricsCalculator(computeTime,repository,task,entityType,em,platformEm,null,null,null,primaryKeyColumnNames,columnInfoList,dataobjectTypeName); 
 
                calculator.init();

                for(Map<String,String> searchResult : searchResults)
                {
                    String entityId = searchResult.get("id");
                    calculator.setEntityId(entityId);

                    Map<String,Double> metricsValues = calculator.calculate();

                    Map<String,String> entityColumnValues = calculator.getEntityColumnValues();

                    map = new HashMap<>();
                    map.putAll(entityColumnValues);
                    
                    Map<String,String> newMetricsValues = getNewMetricsValues(metricsValues,platformEm);
                    map.putAll(newMetricsValues);

                    list.add(map);
                }
            }
            else // from sql
            {*/
                DatasourceConnection datasourceConnection = Util.getRepositorySQLdbDatasourceConnection(em,platformEm,repository.getId()); 

                String schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
                if ( schema.equals("%") )
                    schema = "";

                dbConn = JDBCUtil.getJdbcConnection(datasourceConnection);

                log.info("sql db conn = "+dbConn);

                String tableName = dataobjectType.getName(); //Util.getDataobjectTableName(em,platformEm, task.getOrganizationId(), dataobjectType.getId(),dataobjectType.getName(),repository.getId());

                List<String> primaryKeyFieldNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());

                String primaryKeyFieldNamesStr = "";
                List<String> tableColumnNames = new ArrayList<>();
                for(String primaryKeyFieldName : primaryKeyFieldNames)
                {
                    String tableColumnName = Tool.changeTableColumnName(dataobjectType.getName(),primaryKeyFieldName,true);
                    tableColumnNames.add(tableColumnName);
                    primaryKeyFieldNamesStr+= tableColumnName+",";
                }

                if ( !primaryKeyFieldNamesStr.isEmpty() )
                    primaryKeyFieldNamesStr = primaryKeyFieldNamesStr.substring(0, primaryKeyFieldNamesStr.length()-1);
                   
                String dataobjectTypeName = dataobjectType.getName();
         
                String getEntityValuesSqlStr = String.format("select * from %s%s ",schema,tableName);
          
                List<String> columnInfoList = new ArrayList<>();
                Util.getSingleDataobjectTypeMetadataNames(columnInfoList,dataobjectType.getMetadatas(),true);
            
                String querySQL = "";
                     
                DatabaseType databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);
                
                if ( databaseType == DatabaseType.MYSQL )
                    querySQL = String.format("select %s from %s%s order by %s limit %d,%d",primaryKeyFieldNamesStr,schema,tableName,primaryKeyFieldNamesStr,0,numberOfTrialData);
                else
                if ( databaseType == DatabaseType.GREENPLUM )
                    querySQL = String.format("select %s from %s%s order by %s limit %d offset %d",primaryKeyFieldNamesStr,schema,tableName,primaryKeyFieldNamesStr,numberOfTrialData,0);
                         
                statement = dbConn.prepareStatement(querySQL);
                statement.setFetchSize(500);

                log.info(" 11111111111 querySQL = "+querySQL);

                resultSet = JDBCUtil.executeQuery(statement);
                         
                EntityMetricsCalculator calculator = new EntityMetricsCalculator(new Date(),repository,task,entityType,em,platformEm,datasourceConnection,dbConn,getEntityValuesSqlStr,tableColumnNames,columnInfoList,dataobjectTypeName);

                calculator.init();

                while(resultSet.next())
                {  
                    primaryKeyValue = "";

                    for(String tableColumnName : tableColumnNames)
                        primaryKeyValue += resultSet.getString(tableColumnName)+"~";

                    primaryKeyValue = primaryKeyValue.substring(0, primaryKeyValue.length()-1);

                    calculator.setEntityKey(primaryKeyValue);

                    Map<String,Double> metricsValues = calculator.calculate();

                    Map<String,String> entityColumnValues = calculator.getEntityColumnValues();

                    map = new HashMap<>();
                    map.putAll(entityColumnValues);
                    
                    Map<String,String> newMetricsValues = getNewMetricsValues(metricsValues,platformEm);
                    map.putAll(newMetricsValues);

                    list.add(map);
                }
            //}            
        }
        catch(Exception e)
        {
            log.error(" preCalculateTag() failed! e="+e);
            throw e;
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(dbConn);
        }
                
        return list;        
    }
    
    private static Map<String,String> getNewMetricsValues(Map<String,Double> metricsValues,EntityManager platformEm)
    {
        Map<String,String> newMetricsValues = new HashMap<>();
        
        for(Map.Entry<String,Double> entry : metricsValues.entrySet())
        {
            String[] vals = entry.getKey().split("\\~");
            newMetricsValues.put(vals[0],String.valueOf(entry.getValue()));
        }
        
        return newMetricsValues;
    }
    
    public static List<Map<String,String>> preCalculateTag(EntityManager em,EntityManager platformEm,int entityTypeId, TagProcessingTask task, int numberOfTrialData) throws Exception
    {
        List<Map<String,String>> list = new ArrayList<>();
        Map<String,String> map = new HashMap<>();
        String primaryKeyValue;
        Connection dbConn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        
        try
        {
            EntityType entityType = platformEm.find(EntityType.class, entityTypeId);
            Repository repository = em.find(Repository.class, task.getRepositoryId());
            DataobjectType dataobjectType = platformEm.find(DataobjectType.class,entityType.getDataobjectType());

            DatasourceConnection datasourceConnection = Util.getRepositorySQLdbDatasourceConnection(em,platformEm,repository.getId()); 

            String schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
            if ( schema.equals("%") )
                schema = "";

            dbConn = JDBCUtil.getJdbcConnection(datasourceConnection);

            log.info("sql db conn = "+dbConn);

            String tableName = Util.getDataobjectTableName(em,platformEm, task.getOrganizationId(), dataobjectType.getId(),dataobjectType.getName(),repository.getId());

            List<String> primaryKeyFieldNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());

            String primaryKeyFieldNamesStr = "";
            List<String> tableColumnNames = new ArrayList<>();
            for(String primaryKeyFieldName : primaryKeyFieldNames)
            {
                String tableColumnName = Tool.changeTableColumnName(dataobjectType.getName(),primaryKeyFieldName,true);
                tableColumnNames.add(tableColumnName);
                primaryKeyFieldNamesStr+= tableColumnName+",";
            }

            if ( !primaryKeyFieldNamesStr.isEmpty() )
                primaryKeyFieldNamesStr = primaryKeyFieldNamesStr.substring(0, primaryKeyFieldNamesStr.length()-1);

            String dataobjectTypeName = dataobjectType.getName();

            String getEntityValuesSqlStr = String.format("select * from %s%s ",schema,tableName);

            List<String> columnInfoList = new ArrayList<>();
            Util.getSingleDataobjectTypeMetadataNames(columnInfoList,dataobjectType.getMetadatas(),true);

            String querySQL = String.format("select %s from %s%s order by %s limit %d offset %d",primaryKeyFieldNamesStr,schema,tableName,primaryKeyFieldNamesStr,numberOfTrialData,0);

            statement = dbConn.prepareStatement(querySQL);
            statement.setFetchSize(500);

            log.info(" 11111111111 querySQL = "+querySQL);

            resultSet = JDBCUtil.executeQuery(statement);

            EntityTagProcessor tagProcessor = new EntityTagProcessor(task,entityType,em,platformEm,datasourceConnection,dbConn,getEntityValuesSqlStr,tableColumnNames,columnInfoList,dataobjectTypeName);
            tagProcessor.init();

            while(resultSet.next())
            {  
                primaryKeyValue = "";

                for(String tableColumnName : tableColumnNames)
                    primaryKeyValue += resultSet.getString(tableColumnName)+"~";

                if ( primaryKeyValue.isEmpty() || primaryKeyValue.length() == tableColumnNames.size() )
                    continue;

                primaryKeyValue = primaryKeyValue.substring(0, primaryKeyValue.length()-1);

                tagProcessor.setEntityKey(primaryKeyValue);

                Map<String,String> tagValues = tagProcessor.calculateTag();

                Map<String,String> entityColumnValues = tagProcessor.getEntityColumnValues();

                map = new HashMap<>();
                map.putAll(entityColumnValues);

                tagValues = getNewTagValues(tagValues,platformEm);
                map.putAll(tagValues);

                list.add(map);
            }     
        }
        catch(Exception e)
        {
            log.error(" preCalculateTag() failed! e="+e);
            throw e;
        }
        finally
        {
            JDBCUtil.close(resultSet);
            JDBCUtil.close(dbConn);
        }
                
        return list;        
    }
    
    private static Map<String,String> getNewTagValues(Map<String,String> tagValues,EntityManager platformEm)
    {
        Map<String,String> newTagValues = new HashMap<>();
        
        for(Map.Entry<String,String> entry : tagValues.entrySet())
        {
            String tagDefinitionIdStr = entry.getKey().substring(0, entry.getKey().indexOf("-"));
            String tagShortName = entry.getKey().substring(entry.getKey().indexOf("-")+1);
        
            String sql = String.format("from TagValue where tagDefinitionId=%d and value='%s'",Integer.parseInt(tagDefinitionIdStr),entry.getValue());
            
            TagValue tagValue = (TagValue)platformEm.createQuery(sql).getSingleResult();
            
            newTagValues.put(tagShortName, String.format("%s-%s",entry.getValue(),tagValue.getDescription()));
        }
        
        return newTagValues;
    }
}
