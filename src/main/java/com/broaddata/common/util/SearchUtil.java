/*
 * SearchUtil.java
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.elasticsearch.client.Client;
import java.util.Arrays;
import java.util.Date;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.json.JSONException;
import org.json.JSONObject;

import com.broaddata.common.model.organization.AnalyticQuery;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.thrift.dataservice.Dataset;
import com.broaddata.common.thrift.dataservice.DatasetInfo;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import com.broaddata.common.thrift.dataservice.SearchResponse;

import com.broaddata.common.model.enumeration.DatasetSourceType;
import com.broaddata.common.model.enumeration.DataviewType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.ParentChildSearchType;
import com.broaddata.common.model.enumeration.RowSelectType;
import com.broaddata.common.model.enumeration.SearchScope;
import com.broaddata.common.model.enumeration.SearchType;
import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.DataobjectTypeAssociation;
import com.broaddata.common.thrift.dataservice.DataService;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.lang.exception.ExceptionUtils;

public class SearchUtil 
{
    static final Logger log = Logger.getLogger("SearchUtil");
    
    private static Map<String,Object> objectCache = new HashMap<>();
      
    public static Dataset getDataviewData(int organizationId,EntityManager em,EntityManager platformEm,ByteBuffer dataviewBuf,int dataviewId, Map<String, String> parameters, int fetchSize, int keepLiveSeconds, boolean needDatasetInfo) throws IOException, Exception, ClassNotFoundException 
    {
        AnalyticDataview dataview;
        Map<String, Object> ret;
        List<DataobjectVO> dataobjectVOs;
        long totalHits;
        Dataset dataset = null;
        boolean hasMoreData;
        String repositoryIdStr = "";
        String catalogIdStr = "";
        String storeProcedureParameterValues = null;      
        String currentTime = null;
        
        if ( dataviewBuf == null )
        {
            dataview = em.find(AnalyticDataview.class, dataviewId);
            em.detach(dataview);
        }
        else
            dataview = (AnalyticDataview)Tool.deserializeObject(dataviewBuf.array());
                
        if ( dataview.getType() == DataviewType.NORMAL.getValue() || dataview.getType() == DataviewType.AGGREGATION.getValue() )
        {
            repositoryIdStr = String.format("%s",dataview.getRepositoryId());
            catalogIdStr =String.format("%s",dataview.getCatalogId());
            
            if ( parameters != null )
            {
                String newFilterStr = parameters.get("newFilterStr");
                
                if ( newFilterStr != null && !newFilterStr.trim().isEmpty() )
                    dataview.setFilterString(newFilterStr);  
            }
            
            if ( parameters != null )
                currentTime = parameters.get("currentTime");
        }
        
        if ( dataview.getType() == DataviewType.NORMAL.getValue() )
        {
            String lastStoredTimeStr = "";
            
            if ( parameters != null )
                lastStoredTimeStr = parameters.get("last_stored_time");
            
            if ( lastStoredTimeStr != null && !lastStoredTimeStr.trim().isEmpty() )
            {
                Date date = Tool.convertStringToDate(lastStoredTimeStr, "yyyy-MM-dd HH:mm:ss");
                String time = Tool.convertDateToDateStringForES(date);
                time = Tool.getEndDatetime(time);
                String filter = String.format("last_stored_time:{%s TO *]",time); //{2017-07-03T06:51:23.999 TO *]
                
                if ( dataview.getFilterString() == null || dataview.getFilterString().trim().isEmpty() )
                    dataview.setFilterString(filter);
                else
                {
                    filter = String.format("( %s ) AND %s",dataview.getFilterString().trim(),filter);
                    dataview.setFilterString(filter);
                }
                
                dataview.setColumns(String.format("%s;last_stored_time",dataview.getColumns()));
                dataview.setSortColumns("last_stored_time,ASC");
            }
            
            log.info(" search 111111111111111");
            
            ret = SearchUtil.getDataviewDatasetWithScroll(organizationId, em, platformEm, dataview, fetchSize, keepLiveSeconds,currentTime);
            
            dataobjectVOs = (List<DataobjectVO>)ret.get("dataobjectVOs");
            totalHits = (Long)ret.get("totalHits");
            
            dataset = SearchUtil.convertDataobjectToDataset(platformEm,organizationId,dataview,dataobjectVOs,needDatasetInfo,totalHits);
            
            hasMoreData = (Boolean)ret.get("hasMoreData");
            
            if ( hasMoreData )
            {
                dataset.setHasMoreData(hasMoreData);
                
                String scrollId = (String)ret.get("scrollId");
                dataset.setScrollId(scrollId);
            }
        }
        else
        if ( dataview.getType() == DataviewType.AGGREGATION.getValue() )
        {
            List<DataobjectVO> dataobjects = SearchUtil.getDataviewDataset(organizationId, em, platformEm, dataview, 0, CommonKeys.MAX_EXPECTED_EXCUTION_TIME, null,currentTime);
            dataset = SearchUtil.convertDataobjectToDataset(platformEm,organizationId,dataview,dataobjects,needDatasetInfo,dataobjects.size());
            dataset.setHasMoreData(false);
        }
        else // SQL style and  external db
        if ( dataview.getType() == DataviewType.SQL_STYLE.getValue() )
        {
            if ( parameters != null && !parameters.isEmpty() )
            {
                String newSqlStr = parameters.get("newSqlStr");

                if ( newSqlStr != null && !newSqlStr.trim().isEmpty() )
                    dataview.setSqlStatement(newSqlStr);
            }
             
            DatasourceConnection datasourceConnection = Util.getDatasourceConnectionById(em, dataview.getOrganizationId(), dataview.getRepositoryId());
           
            if ( datasourceConnection.getDatasourceType() == CommonKeys.DATASOURCE_TYPE_FOR_DATABASE )
            {
                dataset = DataviewUtil.getSQLStyleDataviewDataset(datasourceConnection,dataview,platformEm,em);
            }
            else
            if ( datasourceConnection.getDatasourceType() == CommonKeys.DATASOURCE_TYPE_FOR_HBASE )
            {
                String[] selectColumnNames = SearchUtil.getSelectColumnNames(dataview.getSqlStatement());
                dataset = Util.executeHBaseSQL(datasourceConnection,dataview.getSqlStatement(),selectColumnNames,needDatasetInfo,dataview.getStartRow(),dataview.getExpectedRowNumber());
            }
                        
            dataset.setHasMoreData(false);
        }
        else
        {
            if ( parameters != null && !parameters.isEmpty() )
                storeProcedureParameterValues = parameters.get("storeProcedureParameterValues");
 
            dataset = DataviewUtil.getStoreProcedureDataviewDataset(dataview,platformEm,em,storeProcedureParameterValues);
            dataset.setHasMoreData(false);
        }
        
        if ( dataview.getNewColumnData() != null && dataview.getNewColumnData().length > 0 ) // 处理 动态新增字段
            DataviewUtil.processAddNewColumn(em,dataset,dataview,needDatasetInfo);
                
        if ( dataview.getType() == DataviewType.NORMAL.getValue() || dataview.getType() == DataviewType.AGGREGATION.getValue() )
        {
            DataviewUtil.addAssociatedDataToDataview(dataset,dataview,em,platformEm,needDatasetInfo);
            
            if ( needDatasetInfo == true)
                DataviewUtil.selectOutputColumnsAndOrder(dataset,dataview,platformEm);
        }
        
        if ( needDatasetInfo == true)
        {
            Map<String,String> map = new HashMap<>();
            map.put("repositoryId", repositoryIdStr);
            map.put("catalogId",catalogIdStr);
            
            if ( dataset.getDatasetInfo() != null )
                dataset.getDatasetInfo().setProperties(map);
        }
        
        return dataset;
    }
    
    public static Dataset getRemainingDataviewData(int organizationId, EntityManager em, EntityManager platformEm, ByteBuffer dataviewBuf, int dataviewId, String scrollId, int keepLiveSeconds, boolean needDatasetInfo) throws Exception, IOException, ClassNotFoundException 
    {
        Dataset dataset = null;
        
        AnalyticDataview dataview;
        
        if ( dataviewBuf == null )
            dataview = em.find(AnalyticDataview.class, dataviewId);
        else
            dataview = (AnalyticDataview)Tool.deserializeObject(dataviewBuf.array());
        
        if ( dataview.getType() == DataviewType.NORMAL.getValue() )
        {
            Client esClient = ESUtil.getClient(em,platformEm,dataview.getRepositoryId(),false);
            Map<String, Object> searchRet = ESUtil.retrieveRemainingDataset(scrollId, keepLiveSeconds, esClient);
            List<Map<String,Object>> searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");
            
            long totalHits = (Long)searchRet.get("totalHits");
                
            dataset = SearchUtil.convertSearchResultToDataset(platformEm,organizationId,dataview,searchResults,needDatasetInfo,totalHits);
            dataset.setScrollId(scrollId);
            
            int currentHits = (Integer)searchRet.get("currentHits");
            if ( currentHits > 0 )
                dataset.setHasMoreData(true);
            else
                dataset.setHasMoreData(false);
        
            DataviewUtil.addAssociatedDataToDataview(dataset,dataview,em,platformEm,needDatasetInfo);
        }
                    
        if ( dataview.getNewColumnData() != null && dataview.getNewColumnData().length > 0 ) // 处理 动态新增字段
            DataviewUtil.processAddNewColumn(em,dataset,dataview,needDatasetInfo);
                          
        if ( needDatasetInfo == true)
                DataviewUtil.selectOutputColumnsAndOrder(dataset,dataview,platformEm);
       
        return dataset;
    }
    
    public static Map<String,String> getParametersFromDeleteSQLLikeString(String sqlLikeString)
    {
        String indexTypeName;
        String filter = "";
        Map<String,String> parameters = new HashMap<>();
        
        String sql = Util.preProcessSQLLikeString(sqlLikeString);
   
        if ( !sql.contains("delete") || !sql.contains("from") )
        {
            return null;
        }
        
        if ( sql.contains("where"))
        {
            indexTypeName = Tool.extractStr(sql,"from","where");
            filter = Tool.extractStr(sql,"where",null);
            
            if ( filter == null || filter.trim().isEmpty() )
                return null;
        }
        else
        {
            indexTypeName = Tool.extractStr(sql,"from",null);
        }
            
        parameters.put("indexTypeName",indexTypeName);
        parameters.put("filter",filter);       
            
        return parameters;
    }
    
    public static SearchRequest convertSQLLikeStringToRequest(int organizationId, int repositoryId,int catalogId,String sqlLikeString,int searchFrom, int maxExpectedResultSize)
    {
        SearchType searchType;

        String selectedColumnStr;
        String selectedAggregationColumnStr;
        String fromStr;
        String whereStr;
        String groupByStr;
        String orderByStr;
                
        SearchRequest request = new SearchRequest();
 
        request.setOrganizationId(organizationId);
        request.setRepositoryId(repositoryId);
        request.setCatalogId(catalogId);
        
        String sql = Util.preProcessSQLLikeString(sqlLikeString);
        //log.info("sql="+sql);
        if ( sql.contains("group by") || sql.contains("count(") || sql.contains("sum(") || sql.contains("avg(") || sql.contains("max(") || sql.contains("min(") )
        {
            searchType = SearchType.SEARCH_FOR_AGGREGATION;
            
            if ( sql.contains("where"))
                fromStr = Tool.extractStr(sql,"from","where");
            else
            if ( sql.contains("group by"))
                fromStr = Tool.extractStr(sql,"from","group by");
            else
                fromStr = Tool.extractStr(sql,"from","");
         
            if ( sql.contains("group by") )
            {
                if ( sql.contains("order by") )
                    groupByStr = Tool.extractStr(sql,"group by","order by");
                else
                    groupByStr = Tool.extractStr(sql,"group by","");
            }
            else
            {
                groupByStr = "";
            }
            
            request.setGroupby(groupByStr);
            
            selectedAggregationColumnStr = Tool.extractStr(sql,"select","from");
            request.setAggregationColumns(selectedAggregationColumnStr);
                 
            if ( sql.contains("where") )
            {                    
                if ( sql.contains("group by") )
                    whereStr = Tool.extractStr(sql,"where","group by");
                else
                if ( sql.contains("order by") )
                    whereStr = Tool.extractStr(sql,"where","order by");
                else
                    whereStr = Tool.extractStr(sql,"where","");
                
                whereStr = whereStr.replace(" and ", " AND ");
                whereStr = whereStr.replace(" or ", " OR ");
            }
            else
                whereStr = "";            
            
            request.setQueryStr(whereStr);   
        }
        else
        {
            searchType = SearchType.SEARCH_GET_ONE_PAGE;
            
            if ( sql.contains("where"))
                fromStr = Tool.extractStr(sql,"from","where");
            else
            if ( sql.contains("order by") )
                fromStr = Tool.extractStr(sql,"from","order by");
            else
            if ( sql.contains("limit") )
                fromStr = Tool.extractStr(sql,"from","limit");
            else
                fromStr = Tool.extractStr(sql,"from","");
                                            
            selectedColumnStr = Tool.extractStr(sql,"select","from");
            
            if ( selectedColumnStr.equals("*") || (selectedColumnStr.indexOf("dataobject_id")!=-1) )
                selectedColumnStr = "";
            
            request.setColumns(selectedColumnStr);
                                
            if ( sql.contains("where") )
            {
                if ( sql.contains("order by") )
                    whereStr = Tool.extractStr(sql,"where","order by");
                else
                    whereStr = Tool.extractStr(sql,"where","");
                
                whereStr = whereStr.replace(" and ", " AND ");
                whereStr = whereStr.replace(" or ", " OR ");
            }
            else
                whereStr = "";
            
            request.setQueryStr(whereStr);
        }
        
        request.setFilterStr("");
        request.setSearchType(searchType.getValue());    
        request.setTargetIndexTypes(Arrays.asList(new String[]{fromStr}));  
        
        request.setSearchFrom(searchFrom);
        request.setMaxExpectedHits(maxExpectedResultSize);
        request.setMaxExecutionTime(0);
        
        if ( sql.contains("order by") )
        {
            orderByStr = Tool.extractStr(sql,"order by","");
            
            String vals[] = orderByStr.split("\\;");
            
            orderByStr = "";
            for(String val : vals)
            {
                if ( val.toLowerCase().contains("asc") )
                {
                    val = val.replace(" asc", ",ASC;");
                    val = val.replace(" ASC", ",ASC;");
                    orderByStr += val;
                }   
                else
                if ( val.toLowerCase().contains("desc") )
                {
                    val = val.replace(" desc", ",DESC;");
                    val = val.replace(" DESC", ",DESC;");
                    orderByStr += val;
                }   
                else
                    orderByStr += val +",ASC;";
            }
            
            if ( !orderByStr.isEmpty() )
                orderByStr = orderByStr.substring(0, orderByStr.length()-1);
            
            request.setSortColumns(orderByStr);
        }
        
        //log.info(" request = "+request);
                    
        return request;
    }
    
    public static String[] getSelectColumnNames(String sqlStr)
    {
        List<String> selectColumnNames = new ArrayList<>();
               
        String str = sqlStr.replace("SELECT", "select");
        str = str.replace("FROM", "from");
           
        String selectedColumnStr = Tool.extractStr(str,"select","from");
        
        return selectedColumnStr.split("\\;");
    }
          
    public static Dataset convertDataobjectToDataset(EntityManager platformEm, int organizationId, AnalyticDataview dataview,List<DataobjectVO> dataobjectVOs, boolean needDatasetInfo,long totalHits) throws Exception, JSONException 
    {
        boolean found;
        String description = "";
        String type = "";
        String len = "";
        String selectFrom = "";
        String[] selectedColumns = new String[]{};
        Dataset dataset = new Dataset();
        DatasetInfo datasetInfo; 
        Map<String,Map<String,String>> columnValueNameMapping;
        
        DataobjectType dataobjectType = Util.getDataobjectType(platformEm,dataview.getDataobjectTypeId()); 
        
        List<Map<String,Object>> metadataDefintion = new ArrayList<>();
        Util.getSingleDataobjectTypeMetadataDefinition(metadataDefintion,dataobjectType.getMetadatas());
                        
        if ( dataview.getName() == null )
            dataview.setName("");
                
        if ( dataview.getType() ==  DataviewType.NORMAL.getValue() )
        {
            if ( dataview.getColumns() != null && !dataview.getColumns().trim().isEmpty() )
                selectedColumns = dataview.getColumns().trim().split("\\;");
          
            if ( needDatasetInfo )
            {
                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);
                
                columnValueNameMapping = Util.getColumnValueNameMapping(dataobjectType,metadataDefintion,selectedColumns);
                datasetInfo.setColumnValueNameMapping(columnValueNameMapping);
                
                datasetInfo.setDatasetName(dataobjectType.getName()+"-"+dataobjectType.getDescription());
                datasetInfo.setDatasetSourceType(DatasetSourceType.DATAVIEW.getValue());

                datasetInfo.setDatasetSource(dataview.getName());
                
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
                
                for(Map<String,Object> definition : metadataDefintion)
                {
                    String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                    
                    if ( selectedColumns.length > 0)
                    {
                        found = false;

                        for(String cName : selectedColumns)
                        {
                            if ( cName.equals(columnName.trim()) )
                            {
                                found = true;
                                break;
                            }
                        }
                        
                        if ( !found )
                           continue;
                    }
                   
                    description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                    len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);
                    selectFrom = (String)definition.get(CommonKeys.METADATA_NODE_SELECT_FROM);
                    
                    columnNames.add(columnName);
                    
                    Map<String,String> columnInfo = new HashMap<>();
                    columnInfo.put("type", type);
                    columnInfo.put("description",description);
                    columnInfo.put("len", len);
                    columnInfo.put("selectFrom",selectFrom);
                    
                    columnInfoMap.put(columnName, columnInfo);
                }
                
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);
            }
            
            dataset.setRows(new ArrayList<Map<String,String>>());
            
            for(DataobjectVO dataobjectVO : dataobjectVOs)
            {
                Map<String,String> map = new HashMap<>();
                
                if ( selectedColumns.length == 0 )
                {
                    for(Map<String,Object> definition : metadataDefintion)
                    {
                        String columnName = (String)definition.get("name");
                        String columnValue = dataobjectVO.getThisDataobjectTypeMetadatas().get(columnName);

                        if ( columnValue == null )
                            columnValue = "";
                        
                        map.put(columnName, columnValue);
                    }
                }
                else
                {                         
                    for(String cName : selectedColumns)
                    {
                        String columnValue = (String)dataobjectVO.getThisDataobjectTypeMetadatas().get(cName);
                        
                        if ( columnValue == null )
                            columnValue = "";
                        
                        map.put(cName, columnValue);
                    }
                }                
                
                dataset.getRows().add(map);
            }
        }
        else  // aggregation
        {
            if ( dataview.getAggregationColumns() != null && !dataview.getAggregationColumns().trim().isEmpty() )
            {
                if ( dataview.getTimeAggregationColumn() != null && !dataview.getTimeAggregationColumn().trim().isEmpty() )
                    selectedColumns = (dataview.getTimeAggregationColumn().trim()+";"+dataview.getAggregationColumns().trim()).split("\\;");
                else
                    selectedColumns = dataview.getAggregationColumns().trim().split("\\;");
            }
             
            if ( needDatasetInfo )
            {                
                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);
                
                columnValueNameMapping = Util.getColumnValueNameMapping(dataobjectType,metadataDefintion,selectedColumns);
                datasetInfo.setColumnValueNameMapping(columnValueNameMapping);
                
                datasetInfo.setDatasetName(dataobjectType.getName()+"-"+dataobjectType.getDescription());
                datasetInfo.setDatasetSourceType(DatasetSourceType.DATAVIEW.getValue());
                datasetInfo.setDatasetSource(dataview.getName());
                
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
                
                for(String cName : selectedColumns)
                {
                    columnNames.add(cName);
                    
                    if ( cName.contains("count(*)") )
                    {
                        description = "count(*)";
                        type = String.valueOf(MetadataDataType.LONG.getValue());
                        len = String.valueOf(MetadataDataType.LONG.getDefaultLength());
                    }
                    else
                    {
                        for(Map<String,Object> definition : metadataDefintion)
                        {
                            String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                            
                            if ( cName.contains(columnName.trim()) )
                            {
                                if ( cName.contains("(") )
                                    description = String.format("%s_%s",cName.substring(0,cName.indexOf("(")),(String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION));
                                else
                                    description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                                
                                type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                                len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);   
                                selectFrom = (String)definition.get(CommonKeys.METADATA_NODE_SELECT_FROM);
                                break;
                            }
                        }
                    }                                      
                    
                    Map<String,String> columnInfo = new HashMap<>();
                    columnInfo.put("type", type);
                    columnInfo.put("description",description);
                    columnInfo.put("len", len);
                    columnInfo.put("selectFrom",selectFrom);
                                        
                    columnInfoMap.put(cName, columnInfo);
                }
                
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);
            }
            
            dataset.setRows(new ArrayList<Map<String,String>>());
            
            for(DataobjectVO dataobjectVO : dataobjectVOs)
            {               
                Map<String,String> map = new HashMap<>();
                
                for(String cName : selectedColumns)
                {
                    String columnValue = (String)dataobjectVO.getThisDataobjectTypeMetadatas().get(cName);
                    
                    if ( columnValue == null )
                        columnValue = "";
                    
                    map.put(cName, columnValue);
                }
                
                dataset.getRows().add(map);
            }
        }
                
        dataset.setTotalRow(totalHits);
        dataset.setCurrentRow(dataset.getRows().size());
        dataset.setStartRow(0);
        
        return dataset;
    }
 
     public static Dataset convertSearchResultToDataset(EntityManager platformEm, int organizationId, AnalyticDataview dataview, List<Map<String,Object>> searchResults, boolean needDatasetInfo,long totalHits) throws Exception, JSONException 
    {
        boolean found;
        String description = "";
        String type = "";
        String len = "";
        String selectFrom = "";
        String[] selectedColumns = new String[]{};
        Dataset dataset = new Dataset();
        DatasetInfo datasetInfo;
        Map<String,Map<String,String>> columnValueNameMapping;
        JSONObject resultFields;
        Map<String,String> map;
        Map<String,String> result;
        
        DataobjectType dataobjectType = Util.getDataobjectType(platformEm,dataview.getDataobjectTypeId()); 
        
        List<Map<String,Object>> metadataDefintion = new ArrayList<>();
        Util.getSingleDataobjectTypeMetadataDefinition(metadataDefintion,dataobjectType.getMetadatas());
                        
        if ( dataview.getName() == null )
            dataview.setName("");
                
        if ( dataview.getType() ==  DataviewType.NORMAL.getValue() )
        {
            if ( dataview.getColumns() != null && !dataview.getColumns().trim().isEmpty() )
                selectedColumns = dataview.getColumns().trim().split("\\;");
          
            if ( needDatasetInfo )
            {
                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);
                
                columnValueNameMapping = Util.getColumnValueNameMapping(dataobjectType,metadataDefintion,selectedColumns);
                datasetInfo.setColumnValueNameMapping(columnValueNameMapping);
                
                datasetInfo.setDatasetName(dataobjectType.getName()+"-"+dataobjectType.getDescription());
                datasetInfo.setDatasetSourceType(DatasetSourceType.DATAVIEW.getValue());

                datasetInfo.setDatasetSource(dataview.getName());
                
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
                
                for(Map<String,Object> definition : metadataDefintion)
                {
                    String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                    
                    if ( selectedColumns.length > 0)
                    {
                        found = false;

                        for(String cName : selectedColumns)
                        {
                            if ( cName.equals(columnName.trim()) )
                            {
                                found = true;
                                break;
                            }
                        }
                        
                        if ( !found )
                           continue;
                    }
                   
                    description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                    len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);
                    selectFrom = (String)definition.get(CommonKeys.METADATA_NODE_SELECT_FROM);
                    
                    columnNames.add(columnName);
                    
                    Map<String,String> columnInfo = new HashMap<>();
                    columnInfo.put("type", type);
                    columnInfo.put("description",description);
                    columnInfo.put("len", len);
                    columnInfo.put("selectFrom",selectFrom);
                    
                    columnInfoMap.put(columnName, columnInfo);  log.info(" 22222222222222222222 columnNam="+columnName+" info="+columnInfo);
                }
                
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);
            }
            
            dataset.setRows(new ArrayList<Map<String,String>>());
            
            for(Map<String,Object> searchResult : searchResults)
            {
                map = new HashMap<>();
             
                resultFields = new JSONObject((String)searchResult.get("fields"));
                //result = Tool.parseJson(resultFields);
                
                map.put("last_stored_time", resultFields.optString("last_stored_time"));
                
                if ( selectedColumns.length == 0 )
                {
                    for(Map<String,Object> definition : metadataDefintion)
                    {
                        String columnName = (String)definition.get("name");
                        String columnValue = resultFields.optString(columnName);

                        if ( columnValue == null )
                            columnValue = "";
                        
                        map.put(columnName, columnValue);
                    }
                }
                else
                {                         
                    for(String cName : selectedColumns)
                    {
                        String columnValue = resultFields.optString(cName);
                        //String columnValue = result.get(cName);
                        //(String)dataobjectVO.getThisDataobjectTypeMetadatas().get(cName);
                        
                        if ( columnValue == null )
                            columnValue = "";
                        
                        map.put(cName, columnValue);
                    }
                }                
                
                dataset.getRows().add(map);
            }
        }
        else  // aggregation
        {
            if ( dataview.getAggregationColumns() != null && !dataview.getAggregationColumns().trim().isEmpty() )
            {
                if ( dataview.getTimeAggregationColumn() != null && !dataview.getTimeAggregationColumn().trim().isEmpty() )
                    selectedColumns = (dataview.getTimeAggregationColumn().trim()+";"+dataview.getAggregationColumns().trim()).split("\\;");
                else
                    selectedColumns = dataview.getAggregationColumns().trim().split("\\;");
            }
             
            if ( needDatasetInfo )
            {                
                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);
                
                columnValueNameMapping = Util.getColumnValueNameMapping(dataobjectType,metadataDefintion,selectedColumns);
                datasetInfo.setColumnValueNameMapping(columnValueNameMapping);
                
                datasetInfo.setDatasetName(dataobjectType.getName()+"-"+dataobjectType.getDescription());
                datasetInfo.setDatasetSourceType(DatasetSourceType.DATAVIEW.getValue());
                datasetInfo.setDatasetSource(dataview.getName());
                
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
                
                for(String cName : selectedColumns)
                {
                    columnNames.add(cName);
                    
                    if ( cName.contains("count(*)") )
                    {
                        description = "count(*)";
                        type = String.valueOf(MetadataDataType.LONG.getValue());
                        len = String.valueOf(MetadataDataType.LONG.getDefaultLength());
                    }
                    else
                    {
                        for(Map<String,Object> definition : metadataDefintion)
                        {
                            String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                            
                            if ( cName.contains(columnName.trim()) )
                            {
                                if ( cName.contains("(") )
                                    description = String.format("%s_%s",cName.substring(0,cName.indexOf("(")),(String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION));
                                else
                                    description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                                
                                type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                                len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);   
                                selectFrom = (String)definition.get(CommonKeys.METADATA_NODE_SELECT_FROM);
                                break;
                            }
                        }
                    }                                      
                    
                    Map<String,String> columnInfo = new HashMap<>();
                    columnInfo.put("type", type);
                    columnInfo.put("description",description);
                    columnInfo.put("len", len);
                    columnInfo.put("selectFrom",selectFrom);
                                        
                    columnInfoMap.put(cName, columnInfo);
                }
                
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);
            }
            
            dataset.setRows(new ArrayList<Map<String,String>>());
            
            for(Map<String,Object> searchResult : searchResults)
            {
                map = new HashMap<>();
             
                resultFields = new JSONObject(searchResult.get("fields"));
         
                for(String cName : selectedColumns)
                {
                    String columnValue = resultFields.optString(cName);
                    
                    if ( columnValue == null )
                        columnValue = "";
                    
                    map.put(cName, columnValue);
                }
                
                dataset.getRows().add(map);
            }
        }
                
        dataset.setTotalRow(totalHits);
        dataset.setCurrentRow(dataset.getRows().size());
        dataset.setStartRow(0);
        
        return dataset;
    }
     
    public static Dataset convertSearchResponseToDataset(EntityManager platformEm, int organizationId, SearchRequest request, SearchResponse response, boolean needDatasetInfo) throws Exception, JSONException 
    {
        boolean found;
        String description = "";
        String type = "";
        String len = "";
        String selectFrom = "";
        String[] selectedColumns = new String[]{};
        Dataset dataset = new Dataset();
        
        DataobjectType dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, organizationId, request.getTargetIndexTypes().get(0));
        
        //List<Map<String,Object>> metadataDefintion = new ArrayList<>();
        //Util.getSingleDataobjectTypeMetadataDefinition(metadataDefintion,dataobjectType.getMetadatas());
        
        List<Map<String,Object>> metadataDefintion = Util.getDataobjectTypeMetadataDefinition(platformEm,dataobjectType.getId(),true, true);
        
        if ( request.getSearchType() != SearchType.SEARCH_FOR_AGGREGATION.getValue() )
        {
            if ( request.getColumns() != null && !request.getColumns().trim().isEmpty() )
                selectedColumns = request.getColumns().trim().split("\\;");
            // just for a test 
            needDatasetInfo = true;
            if ( needDatasetInfo )
            {
                DatasetInfo datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);
                
                datasetInfo.setDatasetName(dataobjectType.getName()+"-"+dataobjectType.getDescription());
                datasetInfo.setDatasetSourceType(DatasetSourceType.SEARCH.getValue());
                datasetInfo.setDatasetSource("");
                
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
                
                for(Map<String,Object> definition : metadataDefintion)
                {
                    String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                    
                    if ( selectedColumns.length > 0)
                    {
                        found = false;

                        for(String cName : selectedColumns)
                        {
                            if ( cName.equals(columnName.trim()) )
                            {
                                found = true;
                                break;
                            }
                        }
                        
                        if ( !found )
                           continue;
                    }
                   
                    description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                    len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);
                    selectFrom = (String)definition.get(CommonKeys.METADATA_NODE_SELECT_FROM);
                    
                    columnNames.add(columnName);
                    
                    Map<String,String> columnInfo = new HashMap<>();
                    columnInfo.put("type", type);
                    columnInfo.put("description",description);
                    columnInfo.put("len", len);
                    columnInfo.put("selectFrom",selectFrom);
                                       
                    columnInfoMap.put(columnName, columnInfo);
                }
                
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);
            }
            
            dataset.setRows(new ArrayList<Map<String,String>>());
            
            for(Map<String,String> searchResult:response.getSearchResults())
            {
                JSONObject resultFields = new JSONObject(searchResult.get("fields"));
                
                Map<String,String> map = new HashMap<>();
                                                
                map.put("dataobject_id",searchResult.get("id"));
                map.put("dataobject_type_id",String.valueOf(dataobjectType.getId()));
                map.put("index",searchResult.get("index"));
                
                if ( selectedColumns.length == 0 )
                {
                    for(Map<String,Object> definition : metadataDefintion)
                    {
                        String columnName = (String)definition.get("name");
                        String columnValue = (String)resultFields.optString(columnName);

                        columnValue = Tool.newNormalizeNumber(columnValue);
                                                           
                        map.put(columnName, columnValue);
                    }
                }
                else
                {                         
                    for(String cName : selectedColumns)
                    {
                        String columnValue = (String)resultFields.optString(cName);
                        
                        columnValue = Tool.newNormalizeNumber(columnValue);
                           
                        map.put(cName, columnValue);
                    }
                }                
                
                dataset.getRows().add(map);
            }
                     
            dataset.setTotalRow(response.getTotalHits());
        }
        else  // aggregation
        {
            if ( request.getAggregationColumns() != null && !request.getAggregationColumns().trim().isEmpty())
                selectedColumns = request.getAggregationColumns().trim().split("\\;");
            
            if ( needDatasetInfo ) 
            {                
                DatasetInfo datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);
                
                datasetInfo.setDatasetName(dataobjectType.getName()+"-"+dataobjectType.getDescription());
                datasetInfo.setDatasetSourceType(DatasetSourceType.SEARCH.getValue());
                datasetInfo.setDatasetSource("");
                
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
                
                for(String cName : selectedColumns)
                {
                    columnNames.add(cName);
                    
                    if ( cName.contains("count(*)") )
                    {
                        description = "count(*)";
                        type = String.valueOf(MetadataDataType.LONG.getValue());
                        len = String.valueOf(MetadataDataType.LONG.getDefaultLength());
                    }
                    else
                    {
                        for(Map<String,Object> definition : metadataDefintion)
                        {
                            String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                            
                            if ( cName.contains(columnName.trim()) )
                            {
                                if ( cName.contains("(") )
                                    description = String.format("%s_%s",cName.substring(0,cName.indexOf("(")),(String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION));
                                else
                                    description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                                
                                type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                                len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);    
                                selectFrom = (String)definition.get(CommonKeys.METADATA_NODE_SELECT_FROM);
                                break;
                            }
                        }
                    }                                      
                    
                    Map<String,String> columnInfo = new HashMap<>();
                    columnInfo.put("type", type);
                    columnInfo.put("description",description);
                    columnInfo.put("len", len);
                    columnInfo.put("selectFrom",selectFrom);
                    
                    columnInfoMap.put(cName, columnInfo);
                }
                
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);
            }
            
            dataset.setRows(new ArrayList<Map<String,String>>());
            
            for(Map<String,String> searchResult:response.getSearchResults())
            {               
                Map<String,String> map = new HashMap<>();

                for(Map.Entry entry : searchResult.entrySet() )
                {
                    String columnValue = Tool.newNormalizeNumber((String)entry.getValue());
                    map.put((String)entry.getKey(), columnValue);
                }
                            
                dataset.getRows().add(map);
            }
            
            dataset.setTotalRow(dataset.getRows().size());
        }

        dataset.setCurrentRow(dataset.getRows().size());
        dataset.setStartRow(0);
        
        dataset.setScrollId(response.scrollId);
        dataset.setHasMoreData(response.hasMoreData);
        
        return dataset;
    }
 
    public static Dataset getDataviewResult1(EntityManager em,EntityManager platformEm,int organizationId,int dataviewId,int maxExpectedResultSize,Map<String,String> parameters) throws Exception
    {
        Client esClient;
        
        Dataset dataset = new Dataset();
        AnalyticDataview dataview = null;
        AnalyticQuery query = null;
        
        Map<String,Object> searchRet = null;
        List<Map<String,Object>> searchResults;
        Map<String, String> rowResult;

        String sql;
        int repositoryId = 0;
        DataobjectType dataobjectType;
        String currentTime = null;
 
        try
        {
            if ( parameters != null )
                currentTime = parameters.get("currentTime");
            
            dataview = em.find(AnalyticDataview.class, dataviewId);
            
            repositoryId = Integer.parseInt(query.getTargetRepositories());
            
            esClient = ESUtil.getClient(em,platformEm,repositoryId,false);
            
            int catalogId = Integer.parseInt(query.getTargetCatalogs());

            dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm,organizationId, query.getTargetIndexTypes());
            
            String indexKeysStr = query.getTargetIndexKeys();
            String[] indexTypes = new String[]{query.getTargetIndexTypes()};

            indexKeysStr = processIndexKeysStr(indexKeysStr);

            if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
                sql = String.format("select name from IndexSet where catalogId=%d ", catalogId);
            else
                sql = String.format("select name from IndexSet where catalogId =%d and partitionMetadataValue in (%s)", catalogId,indexKeysStr);

            log.debug("sql = "+sql);
            // get index list of target repository

            Query jpaQuery = em.createQuery(sql);
            List<String> indexNameList = jpaQuery.getResultList();

            log.debug("indexnameList="+indexNameList);

            if ( indexNameList.isEmpty() )
            {
                return dataset;
            }
 
            searchRet = ESUtil.retrieveDataset(esClient, indexNameList.toArray(new String[0]), indexTypes, query.getQueryString(), query.getFilterString(), maxExpectedResultSize, 0, null, null, null,currentTime);

            dataset.setTotalRow((Long)searchRet.get("totalHits"));
            dataset.setRows(new ArrayList<Map<String,String>>());
      
            searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");

            for(Map<String,Object> result:searchResults)
            {
                rowResult = Tool.parseJson(new JSONObject((String)result.get("fields"))); 
                dataset.getRows().add(rowResult);
            }
        }
        catch(Exception e)
        {
            log.error(" getQueryResult() failed! e="+e);
            
            if ( e instanceof ClusterBlockException || e instanceof NoNodeAvailableException )
            {
                esClient = ESUtil.getClient(em,platformEm,repositoryId,true);
            }
            
            throw e;
        }
        
        return dataset;
    }
      
    /*
     *  parameters:  "additionalQueryString","xxx"
                     "inputParameterStr","(1): xxxx;(2): yyyyy ;
                     "associationSearchLevel","0"
     *
     */
    public static Dataset getQueryResult(EntityManager em,EntityManager platformEm,int organizationId,int queryId,AnalyticQuery query,int searchFrom,int maxExpectedResultSize,Map<String,String> parameters, boolean needDatasetInfo) throws Exception
    {
        Client esClient;
        DataobjectType dataobjectType;
        
        Dataset dataset = new Dataset();
        Map<String,Object> searchRet = null;
        List<Map<String,Object>> searchResults;

        String sql;
        int repositoryId = 0;
 
        String additionalQueryStr = "";
        String additionalFilterStr = "";
        String inputParameterStr = "";
        String selectedFieldStr = "";
        String[] selectedFields;

        String newQueryStr;
        String newFilterStr;
        String currentTime = null;
                
        try
        {
            if ( parameters != null )
            {
                inputParameterStr = parameters.get("inputParameterStr");
                additionalQueryStr = parameters.get("addtionalQueryStr");
                additionalFilterStr = parameters.get("addtionalFilterStr");
                selectedFieldStr = parameters.get("selectedFields");       
                currentTime = parameters.get("currentTime");
            }
            
            if ( selectedFieldStr == null || selectedFieldStr.trim().isEmpty() )
                selectedFields = new String[]{};
            else
                selectedFields = selectedFieldStr.split("\\,");
                        
            log.info("getQueryResult() queryId="+queryId);
            
            if ( query == null )
                query = em.find(AnalyticQuery.class, queryId);
       
            repositoryId = Integer.parseInt(query.getTargetRepositories());
            
            esClient = ESUtil.getClient(em,platformEm,repositoryId,false);
            
            int catalogId = Integer.parseInt(query.getTargetCatalogs());

            String dataobjectTypeKey = "dataobjectType-"+query.getTargetIndexTypes();
            
            Object obj = objectCache.get(dataobjectTypeKey);
            
            if ( obj == null )
            {       
                dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm,organizationId, query.getTargetIndexTypes());
                objectCache.put(dataobjectTypeKey, dataobjectType);  
            }
            else
                dataobjectType = (DataobjectType)obj;
            
            List<Map<String,Object>> metadataDefintion = new ArrayList<>();
            Util.getSingleDataobjectTypeMetadataDefinition(metadataDefintion,dataobjectType.getMetadatas());
            
            if ( needDatasetInfo )
            {
                DatasetInfo datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);
                
                datasetInfo.setDatasetName(dataobjectType.getName()+"-"+dataobjectType.getDescription());
                datasetInfo.setDatasetSourceType(DatasetSourceType.QUERY.getValue());
                datasetInfo.setDatasetSource(String.valueOf(queryId));
                
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
                
                for(Map<String,Object> definition : metadataDefintion)
                {
                    String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                    String description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);    
                    String type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                    String len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);
                    String selectFrom = (String)definition.get(CommonKeys.METADATA_NODE_SELECT_FROM);
                    
                    columnNames.add(columnName);
                    
                    Map<String,String> columnInfo = new HashMap<>();
                    columnInfo.put("type", type);
                    columnInfo.put("description",description);
                    columnInfo.put("len", len);
                    columnInfo.put("selectFrom",selectFrom);
                    
                    columnInfoMap.put(columnName, columnInfo);
                }
                 
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);   
            }
            
            String indexKeysStr = query.getTargetIndexKeys();
            String[] indexTypes = new String[]{query.getTargetIndexTypes()};

            indexKeysStr = processIndexKeysStr(indexKeysStr);

            if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
                sql = String.format("select i.name from IndexSet i where i.catalogId =%d", catalogId);
            else
                sql = String.format("select i.name from IndexSet i where i.catalogId =%d and i.partitionMetadataValue in (%s)", catalogId,indexKeysStr);

            log.debug("sql = "+sql);
 
            Query jpaQuery = em.createQuery(sql);
            List<String> indexNameList = jpaQuery.getResultList();

            log.debug("indexnameList size="+indexNameList.size());

            if ( indexNameList.isEmpty() )
            {
                return dataset;
            }
                                
            if ( additionalFilterStr == null || additionalFilterStr.trim().isEmpty() )
            {
                if ( query.getFilterString() == null || query.getFilterString().trim().isEmpty() )
                     newFilterStr = "";
                else
                     newFilterStr = query.getFilterString();
            }
            else
            {
                if ( query.getFilterString() == null || query.getFilterString().trim().isEmpty() )
                    newFilterStr = additionalFilterStr;          
                else
                    newFilterStr = String.format("( %s ) AND ( %s )",additionalFilterStr,query.getFilterString());
            }
            
            if ( query.getNeedInputParameters() == 1 )
            {
                // replace filterstr
                
            }
            
            newQueryStr = query.getQueryString();
            
            if ( newQueryStr == null || newQueryStr.trim().isEmpty() )
            {
                if ( additionalQueryStr == null || additionalQueryStr.trim().isEmpty() )
                    newQueryStr = "";
                else
                    newQueryStr = additionalQueryStr;
            }
            else
            {
                if ( !(additionalQueryStr == null || additionalQueryStr.trim().isEmpty()) )
                    newQueryStr = String.format("( %s ) AND ( %s )",newQueryStr,additionalQueryStr);
            }
                
            if (  query.getAssociationQueryData() == null || query.getAssociationQueryData().trim().isEmpty() ) // 无关联查询
            {
                if ( searchFrom + maxExpectedResultSize <= CommonKeys.MAX_EXPECTED_SEARCH_HITS )
                     searchRet = ESUtil.retrieveDataset(esClient, indexNameList.toArray(new String[0]), indexTypes, newQueryStr, newFilterStr, maxExpectedResultSize, searchFrom, selectedFields, null, null,currentTime);
                else  
                     searchRet = ESUtil.retrieveAllDataset(esClient, indexNameList.toArray(new String[0]),indexTypes, newQueryStr, newFilterStr, selectedFields, null, 0,currentTime);
        
                dataset.setRows(new ArrayList<Map<String,String>>());

                searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");

                for(Map<String,Object> result:searchResults)
                {
                    JSONObject resultFields = new JSONObject((String)result.get("fields"));

                    Map<String,String> map = new HashMap<>();
                    
                    map.put("id", (String)result.get("id"));
                    map.put("organization_id", resultFields.optString("organization_id"));
                    map.put("dataobject_type", resultFields.optString("dataobject_type"));
                    map.put("target_repository_id", resultFields.optString("target_repository_id"));
                    
                    if ( selectedFields.length == 0 )
                    {
                        for(Map<String,Object> definition : metadataDefintion)
                        {
                            String columnName = (String)definition.get("name");
                            String columnValue = (String)resultFields.optString(columnName);

                            map.put(columnName, columnValue);    
                        }
                    }
                    else
                    {
                        for(String fieldName : selectedFields)
                        {
                            String columnName = fieldName;
                            String columnValue = (String)resultFields.optString(columnName);

                            map.put(columnName, columnValue);    
                        }
                    }
        
                    dataset.getRows().add(map);
                }
            }
            else
            {               
                List<String> associationQueryList = Arrays.asList(query.getAssociationQueryData().split("\\~"));
                          
                searchRet = ESUtil.retrieveAllDataset(esClient, indexNameList.toArray(new String[0]),indexTypes, newQueryStr, newFilterStr, null, null, 0,currentTime);

                searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");

                List<Map<String,String>> currentRows = new ArrayList<>();

                for(Map<String,Object> result:searchResults)
                {
                    JSONObject resultFields = new JSONObject((String)result.get("fields"));

                    Map<String,String> map = new HashMap<>();
 
                    map.put("id", (String)result.get("id"));
                    
                    map.put("organization_id", resultFields.optString("organization_id"));
                    map.put("dataobject_type", resultFields.optString("dataobject_type"));
                    map.put("target_repository_id", resultFields.optString("target_repository_id"));
                    
                    for(Map<String,Object> definition : metadataDefintion)
                    {
                        String columnName = (String)definition.get("name");
                        String columnValue = (String)resultFields.optString(columnName);

                        map.put(columnName, columnValue);    
                    }

                    currentRows.add(map);
                }

                List<Map<String,String>> remainingRows = processAssociationSearch(esClient,organizationId,em,platformEm,dataobjectType,associationQueryList,currentRows);

                dataset.setRows(remainingRows);
            }
                        
            dataset.setTotalRow(dataset.getRows().size());
            dataset.setCurrentRow(dataset.getRows().size());
            dataset.setStartRow(0);
        }
        catch(Exception e)
        {
            log.error(" getQueryResult() failed! e="+e);
            
            if ( e instanceof ClusterBlockException || e instanceof NoNodeAvailableException )
            {
                ESUtil.getClient(em,platformEm,repositoryId,true);
            }
            
            throw e;
        }
        
        return dataset;
    }
           
    private static DataobjectTypeAssociation getDataobjectTypeAssociation(EntityManager platformEm, int organizationId,int masterDataobjectTypeId, int slaveDataobjectTypeId)
    {
        String sql = String.format("from DataobjectTypeAssociation where organizationId=%d and masterDataobjectTypeId=%d and slaveDataobjectTypeId=%d",organizationId,masterDataobjectTypeId,slaveDataobjectTypeId);       
        DataobjectTypeAssociation dta = (DataobjectTypeAssociation)platformEm.createQuery(sql).getSingleResult();
        
        return dta;
    }
        
    private static List<Map<String,String>>  processAssociationSearch(Client esClient,int organizationId, EntityManager em, EntityManager platformEm,DataobjectType dataobjectType, List<String> associationQueryList,List<Map<String,String>> allRows) throws Exception
    {
        List<Map<String,String>> remainingRows = new ArrayList<>();
        List<Map<String,String>> workingRows = null;
        List<Map<String,String>> currentRows = null;
        String queryStr;
        String filterStr;
        long thisTotalHits;
        boolean needToRemove;
        Map<String,Long> foundTimes;
        DataobjectType slaveDataobjectType;
        DataobjectTypeAssociation dta;
       
        if ( allRows.isEmpty() || associationQueryList.isEmpty()  )
            return remainingRows;
     
        for(String associationQuery : associationQueryList)  // dataobjectId, keyword
        {
            String[] str = associationQuery.split("\\`");
            int associatedDataobjectTypeId = Integer.parseInt(str[0]);
            String keyword = str[1];
            int existingTimes = Integer.parseInt(str[2]);
            int childTypeQueryId = Integer.parseInt(str[3]);
            int parentChildSearchType = Integer.parseInt(str[4]);

            String[] indexNames = new String[]{};

            if ( keyword == null )
                keyword = "";

            String key = String.format("dataobjectType-%d",associatedDataobjectTypeId);
            Object obj = objectCache.get(key);
            
            if ( obj == null )
            {
                slaveDataobjectType = platformEm.find(DataobjectType.class, associatedDataobjectTypeId);
                objectCache.put(key, slaveDataobjectType);
            }
            else
                slaveDataobjectType = (DataobjectType)obj;

            String[] indexTypes = new String[]{slaveDataobjectType.getIndexTypeName().trim()};

            key = String.format("dataobjectTypeAssociation-%d-%d",dataobjectType.getId(),associatedDataobjectTypeId);
            obj = objectCache.get(key);
            
            if ( obj == null)
            {
                dta = getDataobjectTypeAssociation(platformEm, organizationId, dataobjectType.getId(), associatedDataobjectTypeId);
                objectCache.put(key,obj);
            }
            else
                dta = (DataobjectTypeAssociation)obj;
            
            if ( currentRows == null )
                currentRows = allRows;
            else
                currentRows = remainingRows;
            
            int i = 0;
            int j = 0;
            
            for(Map<String,String> row : currentRows)
            {
                i++;
                j++;
                
                if ( i == 1 )
                    workingRows = new ArrayList<>();
                                        
                if ( i <= 950 )
                {
                    workingRows.add(row);
                    
                    if ( j < currentRows.size() )
                        continue;
                }
                
                Map<String,String> ret = Util.getMutipleSlaveFilterStrFromRow(workingRows, dta);
                
                String masterColumnName = ret.get("masterColumnName");
                String slaveColumnName = ret.get("slaveColumnName");
                filterStr = ret.get("filterStr");
                queryStr = keyword;
                
                if ( filterStr.isEmpty() && queryStr.isEmpty() )
                {
                    continue;
                }
    
                int retry = 0;

                while(true)
                {
                    retry++;

                    try
                    {
                        foundTimes = new HashMap<>();
                        
                        if ( childTypeQueryId == 0 )
                        {
                            Map<String,Object> searchRet = ESUtil.retrieveAllDataset(esClient, indexNames, indexTypes, queryStr, filterStr, new String[]{masterColumnName,slaveColumnName}, null, Integer.MAX_VALUE,null);

                            List<Map<String,Object>> searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");

                            for(Map<String,Object> result:searchResults)
                            {
                                JSONObject resultFields = new JSONObject((String)result.get("fields"));
 
                                key = resultFields.optString(slaveColumnName);
                                
                                Long f = foundTimes.get(key);
                                
                                if ( f == null )
                                    foundTimes.put(key, 1l);
                                else
                                    foundTimes.put(key,f+1);
                            }
                        }
                        else
                        {           
                            Map<String,String> parameters = new HashMap<>();

                            parameters.put("addtionalQueryStr", queryStr);
                            parameters.put("addtionalFilterStr",filterStr);
                            parameters.put("selectedFields",masterColumnName+","+slaveColumnName);

                            //Dataset dataset = searchSession.getQueryResult(sessionContext.getOrganizationId(), childTypeQueryId, 0, Integer.MAX_VALUE, parameters, false);

                            Dataset dataset = getQueryResult(em, platformEm, organizationId, childTypeQueryId, null, 0, Integer.MAX_VALUE, parameters, false);

                            for(Map<String,String> map: dataset.getRows())
                            {
                                Long f = foundTimes.get(map.get(slaveColumnName));
                                
                                if ( f == null )
                                    foundTimes.put(map.get(slaveColumnName), 1l);
                                else
                                    foundTimes.put(map.get(slaveColumnName),f+1);
                            }
                        } 
                        
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error(" child search failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                        Tool.SleepAWhileInMS(3*1000);

                        if ( retry > 10 )
                            throw e;
                    }
                }

                for( Map<String,String> rowData : workingRows )
                {
                    Long f = foundTimes.get(rowData.get(masterColumnName));

                    if ( f == null )
                        thisTotalHits = 0;
                    else
                        thisTotalHits = f;

                    needToRemove = false;

                    if ( parentChildSearchType == ParentChildSearchType.MUST_EXIST_IN_CHILD.getValue() )
                    {                        
                        if ( thisTotalHits < existingTimes )
                        {
                            needToRemove = true;
                        }
                    }
                    else // MUST_NOT_EXIST_IN_CHILD
                    {
                        if ( thisTotalHits >= existingTimes )
                        {
                            needToRemove = true;
                        }
                    }

                    if ( needToRemove == false )
                        remainingRows.add(rowData);   
                }
                
                i = 0;
            }
        }
        
        return remainingRows;
    }
      
    public static List<DataobjectVO> getDataviewDataset(int organizationId, EntityManager em, EntityManager platformEm, AnalyticDataview dataview, int maxExpectHits, int maxExecutionTime,DataService.Client dataService,String currentTime) throws Exception 
    {
        SearchRequest request;
        SearchResponse response = null;
        List<DataobjectVO> dataobjects;
        int repositoryId;
        int catalogId;
        String indexType;
        
        repositoryId = dataview.getRepositoryId();
        catalogId = dataview.getCatalogId();

        indexType = dataview.getDataobjectIndexType();
                    
        if ( dataview.getType() ==  DataviewType.NORMAL.getValue() )
        {
            request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_GET_ONE_PAGE.getValue());
            
            request.setTargetIndexKeys(null);
            request.setTargetIndexTypes(Arrays.asList(indexType));
            request.setFilterStr(dataview.getFilterString());
            
            request.setSearchFrom(dataview.getStartRow()-1);
            
            if ( maxExpectHits > 0 )
                request.setMaxExpectedHits(maxExpectHits);
            else
                request.setMaxExpectedHits(dataview.getExpectedRowNumber());
            
            request.setMaxExecutionTime(maxExecutionTime);
            request.setColumns(dataview.getColumns());
            request.setSortColumns(dataview.getSortColumns());
            request.setCurrentTime(currentTime);
            
            if ( dataService == null )
                response = getNormalDataobjects(em,platformEm,request);
            else
                response = dataService.getDataobjects(request);
           
            dataobjects = Util.generateDataobjectsForDataset(response.getSearchResults(),false);
        }
        else  // aggregaton
        {
            if ( dataview.getTimeAggregationColumn() == null || dataview.getTimeAggregationColumn().trim().isEmpty() )
            {
                request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_FOR_AGGREGATION.getValue());
                
                request.setTargetIndexKeys(null);
                request.setTargetIndexTypes(Arrays.asList(indexType));
                request.setFilterStr(dataview.getFilterString());
                
                request.setSearchFrom(0);
                request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);                
                
                request.setMaxExecutionTime(maxExecutionTime);
                request.setGroupby(dataview.getGroupBy());
                request.setAggregationColumns(dataview.getAggregationColumns());
                request.setOrderby(dataview.getOrderBy());
                request.setCurrentTime(currentTime);
                
                if ( dataService == null )
                    response = getAggregationDataobjects(em,platformEm,request);
                else
                    response = dataService.getDataobjects(request);
            }
            else  // has time aggregation
            {
                // generate time period list
                Date queryStartTime = Util.getQueryTime(em,platformEm,"startTime",null,dataview,organizationId,repositoryId,catalogId,indexType);
                Date queryEndTime = Util.getQueryTime(em,platformEm,"endTime",null,dataview,organizationId,repositoryId,catalogId,indexType);
                
                List<Map<String,Object>> periodList = Util.getTimePeriod(dataview,queryStartTime,queryEndTime);
                
                List<Map<String,String>> results = new ArrayList<>();
                
                for(Map<String,Object> period : periodList )
                {
                    Date startTime = (Date)period.get("startTime");
                    
                    String timeRangeStr = (String)period.get("timeRangeStr");
                   
                    request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_FOR_AGGREGATION.getValue());

                    request.setTargetIndexKeys(null);
                    request.setTargetIndexTypes(Arrays.asList(indexType));
 
                    if ( dataview.getFilterString() == null || dataview.getFilterString().trim().isEmpty() )
                        request.setFilterStr(String.format("%s",timeRangeStr));
                    else
                        request.setFilterStr(String.format("%s AND %s",dataview.getFilterString(),timeRangeStr));

                    request.setSearchFrom(0);
                    request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
                    
                    request.setMaxExecutionTime(maxExecutionTime);
                    request.setGroupby(dataview.getGroupBy());
                    request.setAggregationColumns(dataview.getAggregationColumns());
                    request.setOrderby(dataview.getOrderBy());

                    if ( dataService == null )
                        response = getAggregationDataobjects(em,platformEm,request);
                    else
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
        }
        return dataobjects;
    } 
    
    public static Map<String,Object> getDataviewDatasetWithScroll(int organizationId, EntityManager em, EntityManager platformEm, AnalyticDataview dataview, int fetchSize, int keepLiveSeconds,String currentTime) throws Exception 
    {
        SearchRequest request;
        SearchResponse response;
        List<DataobjectVO> dataobjects;
        int repositoryId;
        int catalogId;
        String indexType;
        String scrollId = null;
        Map<String,Object> ret = new HashMap<>();
        DataobjectType dataobjectType;
        
        repositoryId = dataview.getRepositoryId();
        catalogId = dataview.getCatalogId();
        indexType = dataview.getDataobjectIndexType();
        
        dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, organizationId, indexType);
        
        request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_GET_ONE_PAGE.getValue());

        int searchScope = dataobjectType.getIsEventData()==1?SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue():SearchScope.NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue();
        request.setSearchScope(searchScope);
            
        request.setTargetIndexKeys(null);
        request.setTargetIndexTypes(Arrays.asList(indexType));
        request.setFilterStr(dataview.getFilterString());

        request.setSearchFrom(dataview.getStartRow()-1);
        request.setMaxExpectedHits(dataview.getExpectedRowNumber());

        request.setMaxExecutionTime(Integer.MAX_VALUE);
        request.setColumns(dataview.getColumns());
        request.setSortColumns(dataview.getSortColumns());
        request.setCurrentTime(currentTime);

        response = getDataobjectsWithScroll(em, platformEm, request, fetchSize, keepLiveSeconds); 
  
        dataobjects = Util.generateDataobjectsForDataset(response.getSearchResults(),false);
            
        ret.put("totalHits",response.getTotalHits());
        ret.put("currentHits",dataobjects.size());
        ret.put("dataobjectVOs",dataobjects);
        ret.put("scrollId",response.getScrollId());
        ret.put("hasMoreData",response.hasMoreData);
        
        return ret;
    } 
    
    public static SearchResponse getDataobjects(EntityManager em, EntityManager platformEm,SearchRequest request) 
    {
        if ( request.getSearchType() != SearchType.SEARCH_FOR_AGGREGATION.getValue() )
            return getNormalDataobjects(em,platformEm,request);
        else
            return getAggregationDataobjects(em,platformEm,request); 
    }
    
    public static SearchResponse getNormalDataobjects(EntityManager em, EntityManager platformEm,SearchRequest request) 
    {
        String sql;
        List<String> indexNameList;
        SearchResponse searchRep;
        String[] aggregationInfo;
        Client esClient;
        Map<String, Object> searchRet;
        List<Map<String, Object>> searchResults;
        Map<String, String> singleResult;
        String[] selectedFields = new String[]{};
        String[] selectedSortFields = new String[]{};
        
        if ( request.getColumns() != null && !request.getColumns().trim().isEmpty())
            selectedFields = request.getColumns().split("\\;");
        
        if ( request.getSortColumns() != null && !request.getSortColumns().trim().isEmpty() )
            selectedSortFields = request.getSortColumns().split("\\;"); 
        
        String indexKeysStr = Tool.getStrFromListWithQuotation(request.getTargetIndexKeys());
        
        if ( request.getSearchScope() == 0 || request.getSearchScope() == SearchScope.ALL_DATA_IN_CATALOG.getValue() )
        {
            indexKeysStr = SearchUtil.processIndexKeysStr(indexKeysStr);
                
            if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
                sql = String.format("select name from IndexSet where catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d)", request.getCatalogId(), request.getRepositoryId());
            else
                sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue in (%s)", request.getCatalogId(),request.getRepositoryId(),indexKeysStr);
        }
        else
        if ( request.getSearchScope() == SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue() ) // event data
        {
            if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
                sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue not in ('0000','000000','00000000')", request.getCatalogId(),request.getRepositoryId());
            else
                sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue in (%s)", request.getCatalogId(),request.getRepositoryId(),indexKeysStr);
        }
        else  // not event data
        {
            sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue in ('0000','000000','00000000')", request.getCatalogId(),request.getRepositoryId());
        }
                 
        log.debug("sql = "+sql);
        
        // get index list of target repository
        Query query = em.createQuery(sql);
        indexNameList = query.getResultList();
        log.debug("indexnameList="+indexNameList);
        
        if ( indexNameList.isEmpty() )
        {
            searchRep = new SearchResponse();
            searchRep.setTotalHits(0);
            searchRep.setSearchResults(new ArrayList<Map<String,String>>());
            return searchRep;
        }
        
        if ( request.getAggregationInfo() == null || request.getAggregationInfo().isEmpty() )
            aggregationInfo =  new String[]{};
        else
            aggregationInfo = request.getAggregationInfo().toArray(new String[1]);
        
        esClient = ESUtil.getClient(em,platformEm,request.getRepositoryId(),false);
        
        SearchType searchType = SearchType.findByValue(request.getSearchType());
       
        if ( searchType == SearchType.SEARCH_GET_ONE_PAGE )
        {
            searchRet = ESUtil.retrieveDataset(esClient, indexNameList.toArray(new String[0]), request.getTargetIndexTypes().toArray(new String[1]), request.getQueryStr(), request.getFilterStr(), request.getMaxExpectedHits(),request.getSearchFrom(),selectedFields,selectedSortFields,aggregationInfo,request.getCurrentTime());
        }
        else //SearchType.SEARCH_GET_ALL_RESULTS
        {
            searchRet = ESUtil.retrieveAllDataset(esClient, indexNameList.toArray(new String[0]), request.getTargetIndexTypes().toArray(new String[1]), request.getQueryStr(), request.getFilterStr(), selectedFields, aggregationInfo, request.getMaxExpectedHits(),request.getCurrentTime());
        }
        
        searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");
        
        searchRep = new SearchResponse();
        searchRep.setTotalHits((Long)searchRet.get("totalHits"));
        searchRep.setCurrentHits((Integer)searchRet.get("currentHits"));
        searchRep.setSearchResults(new ArrayList<Map<String,String>>());
        searchRep.setAggregationResult((String)searchRet.get("aggregationResult"));      
        
        for(Map<String,Object> result:searchResults)
        {
            singleResult = new HashMap<>();
            singleResult.put("id", (String)result.get("id"));
            singleResult.put("fields", (String)result.get("fields"));
            singleResult.put("score",String.valueOf(result.get("score")));
            singleResult.put("index",(String)result.get("index"));
            
            searchRep.getSearchResults().add(singleResult);
        }
        
        return searchRep;
    }
    
    public static SearchResponse getDataobjectsWithScroll(EntityManager em, EntityManager platformEm,SearchRequest request, int fetchSize, int keepLiveSeconds) 
    {
        String sql;
        List<String> indexNameList;
        SearchResponse searchRep;
        String[] aggregationInfo;
        Client esClient;
        Map<String, Object> searchRet;
        List<Map<String, Object>> searchResults;
        Map<String, String> singleResult;
        String[] selectedFields = new String[]{};
        String[] selectedSortFields = new String[]{};
        
        if ( request.getColumns() != null && !request.getColumns().trim().isEmpty())
            selectedFields = request.getColumns().split("\\;");
        
        if ( request.getSortColumns() != null && !request.getSortColumns().trim().isEmpty() )
            selectedSortFields = request.getSortColumns().split("\\;");
        
        String indexKeysStr = Tool.getStrFromListWithQuotation(request.getTargetIndexKeys());
        
        if ( request.getSearchScope() == 0 || request.getSearchScope() == SearchScope.ALL_DATA_IN_CATALOG.getValue() )
        {
            indexKeysStr = SearchUtil.processIndexKeysStr(indexKeysStr);
                
            if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
                sql = String.format("select name from IndexSet where catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d)", request.getCatalogId(), request.getRepositoryId());
            else
                sql = String.format("select name from IndexSet where ( catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue in (%s) ", request.getCatalogId(),request.getRepositoryId(),indexKeysStr);
        }
        else
        if ( request.getSearchScope() == SearchScope.NOT_NULL_PARTITION_KEY_VALUE_DATA_IN_CATALOG.getValue() ) // event data
        {
            if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
                sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue not in ('00000000','000000','0000')",request.getCatalogId(),request.getRepositoryId());
            else
                sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue in (%s)",request.getCatalogId(),request.getRepositoryId(),indexKeysStr);
        }
        else  // not event data
        {
            sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) ) and partitionMetadataValue in ('00000000','000000','0000')", request.getCatalogId(),request.getRepositoryId());
        }
        
        // get index list of target repository
        Query query = em.createQuery(sql);
        indexNameList = query.getResultList();
        log.info("indexnameList="+indexNameList);
        
        if ( indexNameList.isEmpty() )
        {
            log.debug(" sql="+sql);
            log.debug(" indexNameList is empty!");
            
            searchRep = new SearchResponse();
            searchRep.setTotalHits(0);
            searchRep.setSearchResults(null);
            return searchRep;
        }
        
        if ( request.getAggregationInfo() == null || request.getAggregationInfo().isEmpty() )
            aggregationInfo =  new String[]{};
        else
            aggregationInfo = request.getAggregationInfo().toArray(new String[1]);
        
        esClient = ESUtil.getClient(em,platformEm,request.getRepositoryId(),false);
        
        searchRet = ESUtil.retrieveAllDatasetWithScroll(fetchSize,keepLiveSeconds,esClient, indexNameList.toArray(new String[0]), request.getTargetIndexTypes().toArray(new String[1]), request.getQueryStr(), request.getFilterStr(), selectedFields, selectedSortFields,request.getCurrentTime());
             
        searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");
        
        searchRep = new SearchResponse();
          
        long totalHits = (Long)searchRet.get("totalHits");
        int currentHits = (Integer)searchRet.get("currentHits");
               
        if ( currentHits > 0 && currentHits < totalHits )
            searchRep.setHasMoreData(true);
        else
            searchRep.setHasMoreData(false);
      
        searchRep.setTotalHits((Long)searchRet.get("totalHits"));
        searchRep.setCurrentHits(currentHits);
        searchRep.setSearchResults(new ArrayList<Map<String,String>>());
        searchRep.setScrollId((String)searchRet.get("scrollId"));
            
        for(Map<String,Object> result:searchResults)
        {
            singleResult = new HashMap<>();
            singleResult.put("id", (String)result.get("id"));
            singleResult.put("fields", (String)result.get("fields"));
            singleResult.put("index",(String)result.get("index"));
            
            searchRep.getSearchResults().add(singleResult);
        }
        
        return searchRep;
    }
    
    public static SearchResponse getRemainingDataobjects(EntityManager em, EntityManager platformEm,int repositoryId, String scrollId,int keepLiveSeconds) 
    {
        Map<String, String> singleResult;
        SearchResponse searchRep;
        Client esClient;
        Map<String, Object> searchRet;
        List<Map<String, Object>> searchResults;
                
        esClient = ESUtil.getClient(em,platformEm,repositoryId,false);
 
        searchRet = ESUtil.retrieveRemainingDataset(scrollId, keepLiveSeconds, esClient);
          
        searchResults = (List<Map<String,Object>> )searchRet.get("searchResults");
        
        searchRep = new SearchResponse();
          
        int currentHits = (Integer)searchRet.get("currentHits");
                  
        if ( currentHits > 0 )
            searchRep.setHasMoreData(true);
        else
            searchRep.setHasMoreData(false);
      
        searchRep.setTotalHits((Long)searchRet.get("totalHits"));
        searchRep.setCurrentHits(currentHits);
        searchRep.setSearchResults(new ArrayList<Map<String,String>>());
        searchRep.setScrollId((String)searchRet.get("scrollId"));
        
        for(Map<String,Object> result:searchResults)
        {
            singleResult = new HashMap<>();
            singleResult.put("id", (String)result.get("id"));
            singleResult.put("fields", (String)result.get("fields"));
            singleResult.put("index",(String)result.get("index"));
            
            searchRep.getSearchResults().add(singleResult);
        }
        
        return searchRep;
    }
    
    public static SearchResponse getAggregationDataobjects( EntityManager em, EntityManager platformEm,SearchRequest request) 
    {
        String[] groupbyFields = new String[]{};
        String[] aggregationFields = new String[]{};
        String[] orderbyFields = new String[]{};                 
        String sql;
        List<String> indexNameList;
        Client esClient;
        List<Map<String, String>> dataset;
        SearchResponse searchRep;
        
        if ( request.getGroupby() != null && !request.getGroupby().trim().isEmpty())
            groupbyFields = request.getGroupby().trim().split("\\;");
        
        if ( request.getAggregationColumns() != null && !request.getAggregationColumns().trim().isEmpty())
            aggregationFields = request.getAggregationColumns().trim().split("\\;");
        
        if ( request.getOrderby() != null && !request.getOrderby().trim().isEmpty())
            orderbyFields = request.getOrderby().trim().split("\\;");
        
        String indexKeysStr = Tool.getStrFromList(request.getTargetIndexKeys());
        indexKeysStr = SearchUtil.processIndexKeysStr(indexKeysStr);
        
        if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
            sql = String.format("select name from IndexSet where catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d)", request.getCatalogId(),request.getRepositoryId());
        else
            sql = String.format("select name from IndexSet where (catalogId=%d or catalogId in (select dataMartCatalogId from Repository where id=%d) )and partitionMetadataValue in (%s)", request.getCatalogId(),request.getRepositoryId(),indexKeysStr,request.getRepositoryId(),indexKeysStr);
   
        Query query = em.createQuery(sql);
        indexNameList = query.getResultList();
        log.info("indexnameList="+indexNameList);
        
        esClient = ESUtil.getClient(em,platformEm,request.getRepositoryId(),false);
        dataset = ESUtil.retrieveDatasetAggregation(esClient, indexNameList.toArray(new String[1]), request.getTargetIndexTypes().toArray(new String[1]), request.getQueryStr(), request.getFilterStr(), request.getMaxExpectedHits(),request.getSearchFrom(),groupbyFields,aggregationFields,orderbyFields,request.getCurrentTime());
        
        searchRep = new SearchResponse();
        searchRep.setSearchResults(dataset);
        searchRep.setTotalHits(dataset.size());
        searchRep.setCurrentHits(dataset.size());
        
        return searchRep;
    }
     
    public static String processIndexKeysStr(String indexKeysStr)
    {
        if ( indexKeysStr == null || indexKeysStr.trim().isEmpty() )
            return indexKeysStr;
        
        StringBuilder builder = new StringBuilder();
        
        builder.append(indexKeysStr);
        builder.append(",'000000'"); // add for not event data
        
        return builder.toString();
    }    
}
