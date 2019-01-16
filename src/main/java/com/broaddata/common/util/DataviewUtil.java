/*
 * DataviewUtil.java
 *
 */
 
package com.broaddata.common.util;

import org.apache.log4j.Logger;
import javax.persistence.EntityManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.elasticsearch.client.Client;
import org.json.JSONObject;
import java.sql.CallableStatement;
import java.util.UUID;

import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.AnalyticQuery;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DataobjectTypeAssociation;
import com.broaddata.common.thrift.dataservice.Dataset;
import com.broaddata.common.thrift.dataservice.DatasetInfo;
import com.broaddata.common.model.enumeration.DatasetSourceType;
import com.broaddata.common.model.enumeration.DataviewType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.NewColumnComputingType;
import com.broaddata.common.model.enumeration.RowSelectType;
import com.broaddata.common.model.organization.CodeDetailsMapping;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;

public class DataviewUtil 
{
    static final Logger log = Logger.getLogger("DataviewUtil ");

    public static List<Map<String, Object>> getDataviewInputParameters(AnalyticDataview selectedDataview) throws Exception 
    {
        List<Map<String, Object>> queryCriteria = null;
        
        if ( selectedDataview.getType() == DataviewType.NORMAL.getValue() ||  selectedDataview.getType() == DataviewType.AGGREGATION.getValue() )
        {
            if ( selectedDataview.getNeedInputParameters() == 1) // need parameter
                queryCriteria =DataviewUtil.generateDataviewInputParameterData(selectedDataview.getParameters());
        }
        else
        if ( selectedDataview.getType() == DataviewType.SQL_STYLE.getValue() )
         // sql
        {
            log.info("11111 sql = "+ selectedDataview.getSqlStatement());
            
            List<String> parameters;
            
            if ( selectedDataview.getType() == DataviewType.SQL_STYLE.getValue() )
                parameters = DataviewUtil.extraceSqlParameterStrs(selectedDataview.getSqlStatement());
            else
                parameters = DataviewUtil.extraceStoreProcedureParameterStrs(selectedDataview.getSqlStatement(),"in");
            
            log.info(" parameters = "+parameters.size());
            
            if ( !parameters.isEmpty() )
                queryCriteria = DataviewUtil.generateDataviewInputParameterDataForSQL(parameters);
        }

        return queryCriteria;
    }
    
    public static Map<String,String> createDataviewParameters(EntityManager em,int organizationId,ByteBuffer dataviewBuf,int dataviewId,Map<String,String> parameters) throws Exception
    {
        Map<String,String> dataviewParameters;
        AnalyticDataview dataview;
        
        try
        { 
          if ( dataviewBuf == null )
            {
                dataview = em.find(AnalyticDataview.class, dataviewId);
                em.detach(dataview);
            }
            else
                dataview = (AnalyticDataview)Tool.deserializeObject(dataviewBuf.array());
                                  
            if ( dataview.getNeedInputParameters() == 0)
                return null;
            
            List<Map<String,Object>> inputParameters = getDataviewInputParameters(dataview); 
            
            if ( inputParameters == null )
                return parameters;

            for(Map<String,Object> para : inputParameters)
            {
                String cid = (String)para.get("cid");
                int dataType = Integer.parseInt((String)para.get("dataType"));
            
                String inputStr = parameters.get(cid);
                
                if ( inputStr == null )
                    return parameters;
            
                if ( dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue() )
                    para.put("metadataValueDate",Tool.convertStringToDate(inputStr, "MM/dd/yyyy"));  
                else
                    para.put("metadataValue",inputStr);
            }

             dataviewParameters = DataviewUtil.generateDataviewParameters(dataview,organizationId,inputParameters);
        }
        catch(Exception e)
        {
            log.error(" generateDataviewParameters() failed! e="+e+", stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
          
        return dataviewParameters;
    }
        
    public static List<Map<String,Object>> generateDataviewInputParameterDataForSQL(List<String> parameters) throws Exception
    {
        Map<String,Object> singleCriterion;
        
        log.info("generatePanelInputParameterDataForSQL parameters="+parameters);
 
        List<Map<String,Object>> queryCriteria = new ArrayList<>();
        
        for(String parameter : parameters)  // $(1),2286,AC19BRNO,交易机构,eq,1,
        {
            String[] vals = parameter.split("\\~");
            String operatorName = "eq";
            singleCriterion = new HashMap<>();
            
            singleCriterion.put("id", UUID.randomUUID().toString());
            singleCriterion.put("cid", vals[0]);
            
            if ( vals.length > 1)
                singleCriterion.put("description",vals[1]);
            else
                singleCriterion.put("description","");  
            
            singleCriterion.put("operatorName",operatorName);            
            singleCriterion.put("dataType","1");
            singleCriterion.put(CommonKeys.CRITERIA_KEY_singleInputMetadataValue,true);
       
            //String operatorDescription = getBundleMessage(operatorName.toUpperCase());
            //singleCriterion.put("selectedOperator",operatorDescription);           
            //singleCriterion.put("valueSelection",new ArrayList<>());
            
            queryCriteria.add(singleCriterion);
        }
        log.info("11111111111111111 queryCriteria="+queryCriteria);
        return queryCriteria; 
    }
     
    public static List<Map<String,Object>> generateDataviewInputParameterData(String parameters) throws Exception
    {
        Map<String,Object> singleCriterion;
       
        log.info("generatePanelInputParameterData( query="+parameters);

        String[] inputParameters = parameters.trim().split("\\;");
        
        List<Map<String,Object>> queryCriteria = new ArrayList<>();
        
        for(String parameter : inputParameters) //$(1),2,file_ext,鎵╁睍鍚嶏紝eq,1;
        {                                       //$(1),1063,OrderType,OrderType,eq,3
            String[] para = parameter.split("\\,");
            
            singleCriterion = new HashMap<>();
            
            singleCriterion.put("id", UUID.randomUUID().toString());
            singleCriterion.put("cid", para[0]);
            singleCriterion.put("description",para[3]);
            singleCriterion.put("operatorName",para[4]);            
            singleCriterion.put("dataType",para[5]);
            
            String operatorName = para[4];
            if ( operatorName.equals("eq") || operatorName.equals("like") || operatorName.equals("gt") || operatorName.equals("gte")
                    || operatorName.equals("lt") || operatorName.equals("lte"))
                singleCriterion.put(CommonKeys.CRITERIA_KEY_singleInputMetadataValue,true);
            else
                singleCriterion.put(CommonKeys.CRITERIA_KEY_singleInputMetadataValue,false);
                
            String operatorDescription = Util.getBundleMessage(operatorName.toUpperCase());
            singleCriterion.put("selectedOperator",operatorDescription);           
           
            queryCriteria.add(singleCriterion);
        }
        log.info("11111111111111111 queryCriteria="+queryCriteria);
        return queryCriteria; 
    }
    
    public static Map<String,String> generateDataviewParameters(AnalyticDataview selectedDataview,int organizationId,List<Map<String,Object>> dataviewInputParameters) throws Exception
    {
        Map<String,String> dataviewParameters = new HashMap<>();
        
        if ( selectedDataview.getType() == DataviewType.NORMAL.getValue() || selectedDataview.getType() == DataviewType.AGGREGATION.getValue() )
        {
            if ( selectedDataview.getNeedInputParameters() == 1 && dataviewInputParameters != null && !dataviewInputParameters.isEmpty() )
            {
                String newFilterStr = Util.replaceFilterString(selectedDataview.getFilterString(),dataviewInputParameters);
                dataviewParameters.put("newFilterStr",newFilterStr);
            }
        }
        else // for sql
        {
            if ( dataviewInputParameters != null && !dataviewInputParameters.isEmpty() )
            {
                if ( selectedDataview.getType() == DataviewType.SQL_STYLE.getValue() )
                {
                    String newSqlStr = DataviewUtil.replaceSqlString( selectedDataview.getSqlStatement(),dataviewInputParameters);
                    dataviewParameters.put("newSqlStr",newSqlStr);
                }
                else
                {
                    dataviewParameters.put("storeProcedureParameterValues",DataviewUtil.getStoreProcedureParameterValues(dataviewInputParameters));
                }   
            }
        }      
                    
        return dataviewParameters;
    }
    
    public static List<String> getDataviewParameterNames(AnalyticDataview dataview)
    {
        List<String> parameterNames = new ArrayList<>();
                
        if ( dataview.getType() == DataviewType.NORMAL.getValue() || dataview.getType() == DataviewType.AGGREGATION.getValue() )
        {
            if ( dataview.getNeedInputParameters() == 0 )
                return parameterNames;
                    
            String[] inputParameters = dataview.getParameters().trim().split("\\;");
    
            for(String parameter : inputParameters) //$(1),2291,CORE_BDFMHQAC_AC01AC15,账号,eq,1,;
            {
                String[] para = parameter.split("\\,");              
                parameterNames.add(String.format("%s-%s-%s",para[0],para[2],para[3])); 
            }
        }
        else // sql 
        {
            List<String> list = extraceSqlParameterStrs(dataview.getSqlStatement());
            
            for(String parameter : list)  // $(1),2286,AC19BRNO,交易机构,eq,1,
            { 
                String[] vals = parameter.split("\\~");
                
                if ( vals.length == 3 )
                    parameterNames.add(String.format(":%s-%s-%s",vals[0],vals[0],vals[1])); 
                else
                    parameterNames.add(String.format(":%s-%s",vals[0],vals[0])); 
            }
        }
        
        return parameterNames;
    }
    
    public static String replaceSqlWithParameters(String sql,List<String> parameterList)
    {      
        for(String para : parameterList)
        {
            String[] vals = para.split("\\`");
            sql = sql.replace(":"+vals[0], vals[1]);
        }
        
        return sql;
    }
            
    public static String replaceSqlWithParameters1(String sql,List<String> parameterList)
    {
        int k=0;
 
        for(String para : parameterList)
        {
            String[] vals = para.split("\\:");
            
            int i = sql.indexOf(vals[0]);
           
            if ( sql.charAt(i-2) == '\'') 
            {
                for(int j=i-3;j>0;j--)
                {
                    if ( sql.charAt(j)=='\'')
                    {
                        k=j;
                        break;
                    }
                }
                
                sql = String.format("%s '%s' %s",sql.substring(0, k),vals[1],sql.substring(i+vals[0].length()+1));
            }
            else
            {
                for(int j=i-3;j>0;j--)
                {
                    if ( sql.charAt(j) == '=' || sql.charAt(j) == '>' || sql.charAt(j) == '<' || sql.charAt(j) == ' ' )
                    {
                        k=j;
                        break;
                    }
                }
                
                sql = String.format("%s %s %s",sql.substring(0, k+1),vals[1],sql.substring(i+vals[0].length()+1));                
            }
        }
                
        return sql;
    }
    
    public static List<String> extraceSqlParameterStrs(String sql)
    {
        List<String> list = new ArrayList<>();
        String para;
        String newSql;
        
        try
        {
           while(true)
           {
               int i = sql.indexOf(":");
               
               if ( i == -1 )
                   break;
      
               newSql = sql.substring(i+1);
          
               if ( newSql.charAt(0)>='0' && newSql.charAt(0)<='9' || newSql.startsWith("mi") || newSql.startsWith("ss")  )
               {
                   sql = newSql;
                   continue;
               }               
               
               int j = 0;
               for(int k=0;k<newSql.length();k++)
               {
                   j++;
                   if ( newSql.charAt(k) == ',' || newSql.charAt(k) == ' ' || newSql.charAt(k) == '\'' )
                       break;
               }
               
               para = newSql.substring(0, j);
               if ( para.endsWith("'") || para.endsWith(",") || para.endsWith(" ")  )
                   para = para.substring(0, para.length()-1);

               if ( Tool.ifListContainsStr(list,para) == false )
                   list.add(para);
    
               sql = newSql;
           }
        }
        catch(Exception e)
        {
            log.error("extraceSqlParameterStrs() failed! sql="+sql+" e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
              
        return list;
    }
    
    public static String extraceStoreProcedureName(String sql)
    {
        String name = sql.toUpperCase().substring(sql.indexOf("CALL")+7,sql.indexOf("("));
        return name;
    }
    
    public static List<String> extraceStoreProcedureParameterStrs(String sql,String type)
    {
        List<String> list = new ArrayList<>();
        String para;
        String newSql;
        
        // "{call STORE_PROCEDURE_XXXX(?NUMBER,?VARCHAR2,?VARCHAR2)}");
        
        try
        {
           sql = sql.substring(sql.indexOf("call"));
           
           while(true)
           {
               int i = sql.indexOf("?");
               
               if ( i == -1 )
                   break;
      
               newSql = sql.substring(i+1);
                    
               int j = 0;
               for(int k=0;k<newSql.length();k++)
               {
                   j++;
                   if ( newSql.charAt(k) == ',' || newSql.charAt(k) == ' ' || newSql.charAt(k) == ')' )
                       break;
               }
               
               para = newSql.substring(0, j);
               if ( para.endsWith(")") || para.endsWith(",") || para.endsWith(" ")  )
                   para = para.substring(0, para.length()-1);
 
               if ( type == null || type.isEmpty() || para.startsWith(type))
                   list.add(para);
    
               sql = newSql;
           }
        }
        catch(Exception e)
        {
            log.error("extraceStoreProcedureParameterStrs() failed! sql="+sql+" e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
              
        return list;
    }
    /*
               if ( j == -1 )
               {
                   j = newSql.indexOf(",");
                   
                   if ( j == -1 )
                   {
                       para = newSql;
                       if ( para.endsWith("'") )
                           para = para.substring(0, para.length()-1);

                       if ( Tool.ifListContainsStr(list,para) == false )
                           list.add(para);
                      
                       break;
                   }
               }
               */
    public static List<String> extraceSqlParameterStrs1(String sql)
    {
        List<String> list = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        
        try
        {
            boolean in = false;
        
            for(int i=0;i<sql.length();i++)
            {           
                if ( in == true )
                    builder.append(sql.charAt(i));
                else
                {
                    if ( builder.length() > 0 )
                    {
                        String str = builder.toString();
                        str = str.substring(0, str.length()-1);

                        list.add(str); log.info("11111111111 str = "+str);
                        builder = new StringBuilder();
                    }
                }

                if ( sql.charAt(i) == '~' )
                {
                    if ( in == false )
                        in = true;
                    else
                        in = false;       
                }  
            }
            
            if ( builder.length() > 0 )
            {
                String str = builder.toString();
                str = str.substring(0, str.length()-1);

                list.add(str); 
             }
        }
        catch(Exception e)
        {
            log.error("extraceSqlParameterStrs() failed! sql="+sql+" e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
              
        return list;
    }
    
    public static String removeSqlParameterStr(String sql)
    {
        StringBuilder builder = new StringBuilder();
        
        if ( sql == null || sql.isEmpty() )
            return "";
        
        boolean in = false;
        
        for(int i=0;i<sql.length();i++)
        {            
            if ( in == false )
                builder.append(sql.charAt(i));
            
            if ( sql.charAt(i) == '~' )
            {
                if ( in == false )
                {
                    builder.deleteCharAt(builder.length()-1);
                    in = true;
                }
                else
                    in = false;       
            }
        }
        
        return builder.toString();
    }
        
    public static String getStoreProcedureParameterValues(List<Map<String,Object>> queryParameters)
    {
        String para;
        String result = "";
        
        for(Map<String,Object> parameter : queryParameters)
        {
            String cid = (String)parameter.get("cid");
            String metadataValue = (String)parameter.get(CommonKeys.CRITERIA_KEY_metadataValue);
         
            para = String.format("%s`%s",cid,metadataValue);
          
            result += para+",";
        }

        return result;  
    }
    
    public static String replaceSqlString(String sqlStr,List<Map<String,Object>> queryParameters)
    {
        String para;
        List<String> inputParameters = new ArrayList<>();
        
        for(Map<String,Object> parameter : queryParameters)
        {
            String cid = (String)parameter.get("cid");
            String description = (String)parameter.get("description");
            String metadataValue = (String)parameter.get(CommonKeys.CRITERIA_KEY_metadataValue);
            
            if ( description == null || description.trim().isEmpty() )
                para = String.format("%s`%s",cid,metadataValue);
            else
                para = String.format("%s~%s`%s",cid,description,metadataValue);
            
            log.info(" para = "+para);
            inputParameters.add(para);
        }
        
        String newSql = replaceSqlWithParameters(sqlStr, inputParameters);
        
        return newSql;        
    }
    
    public static Dataset getSQLStyleDataviewDataset(DatasourceConnection datasourceConnection,AnalyticDataview dataview, EntityManager platformEm,EntityManager em) throws Exception
    {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        Dataset dataset = new Dataset();
        DatasetInfo datasetInfo;
        int maxSize;
        
        try
        {                 
            String[] selectColumnNames = SearchUtil.getSelectColumnNames(dataview.getSqlStatement());
 
            conn = JDBCUtil.getJdbcConnection(datasourceConnection);
            stmt = conn.createStatement();

            log.info("excuting sql="+dataview.getSqlStatement());
            
            resultSet = stmt.executeQuery(dataview.getSqlStatement());
            stmt.setFetchSize(500);
            
            if ( dataview.getRowSelectType() == RowSelectType.RANGE.getValue() )
                maxSize = dataview.getStartRow()+dataview.getExpectedRowNumber()-1;
            else
                maxSize = dataview.getExpectedRowNumber();
            
            if ( resultSet.next() == true )
            {
                List<String> columnNames = new ArrayList<>();
                Map<String,Map<String,String>> columnInfoMap = new HashMap<>();

                SearchRequest request = SearchUtil.convertSQLLikeStringToRequest(0,0,0,dataview.getSqlStatement(),0,0);  
                String tableName = request.getTargetIndexTypes().get(0);
                
                Map<String,Map<String,String>> tableInfo = JDBCUtil.getDatabaseTableOrViewColumnComment(conn,tableName);
                            
                log.info("table info size="+tableInfo.size());
                
                ResultSetMetaData rsmd = resultSet.getMetaData();
                for(int i=1;i<=rsmd.getColumnCount();i++)
                {
                    String columnName = rsmd.getColumnName(i);
                    
                    if ( columnName.toUpperCase().equals("COUNT") || columnName.toUpperCase().equals("SUM") || columnName.toUpperCase().equals("AVG") ||
                            columnName.toUpperCase().equals("MIN") || columnName.toUpperCase().equals("MAX") )
                    {
                        columnName = selectColumnNames[i-1];
                    }

                    //int len = rsmd.getColumnDisplaySize(i);
                    
                    String comment = "";
                    int len = 0;
                    
                    try
                    {
                        len = Integer.parseInt(tableInfo.get(columnName.toLowerCase()).get("columnLen"));
                    }
                    catch(Exception e)
                    {
                        log.info("555555555 get len comment failed! e="+e+" columnName="+columnName);
                        len = 0;
                    }
                      
                    try
                    {
                        comment = tableInfo.get(columnName.toLowerCase()).get("columnComment");
                    }
                    catch(Exception e)
                    {
                        log.info("555555555 get len comment failed! e="+e+" columnName="+columnName);
                        comment = "";
                    }
                    
                    if ( comment == null )
                        comment = "";
                    
                    if ( len == 0 )
                        len = rsmd.getPrecision(i);
                    
                    int precision =  rsmd.getScale(i);
                    
                    if ( precision < 0 )
                        precision = 0;
                    
                    int type = Util.convertToSystemDataType(String.valueOf(rsmd.getColumnType(i)),String.valueOf(len),String.valueOf(precision));
                   
                    columnNames.add(columnName);

                    Map<String,String> columnInfo = new HashMap<>();

                    columnInfo.put("type", String.valueOf(type));
                    columnInfo.put("description",columnName);
                    columnInfo.put("len", String.valueOf(len));
                    columnInfo.put("precision",String.valueOf(precision));
                    columnInfo.put("comment",comment);

                    columnInfoMap.put(columnName, columnInfo);
                }

                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);

                if ( dataview.getName() == null )
                    dataview.setName("");
                
                datasetInfo.setDatasetName(dataview.getName());
                datasetInfo.setDatasetSourceType(DatasetSourceType.DATAVIEW.getValue());
                datasetInfo.setDatasetSource(dataview.getName());                    

                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);

                dataset.setRows(new ArrayList<Map<String,String>>());

                int k=1;
                
                do
                {
                    if ( dataview.getRowSelectType() == RowSelectType.RANGE.getValue() )
                    {
                        if ( k < dataview.getStartRow() )
                        {
                            k++;
                            continue;
                        }
                    }
                    
                    if ( k > maxSize && maxSize > 0 )
                    {
                        if ( resultSet.next() == false )
                            break;
                        
                        k++;
                        continue;
                    }
                                        
                    Map<String,String> map = new HashMap<>();

                    int kk=1;
                    for(String cName : columnNames)
                    {
                        //String columnValue = resultSet.getString(cName);

                        String columnValue = resultSet.getString(kk);

                        if ( columnValue == null )
                            columnValue = "";

                        map.put(cName, columnValue);
                        kk++;
                    }

                    dataset.getRows().add(map);
                                                  
                    k++;
   
                }
                while(resultSet.next());
                
                dataset.setTotalRow(dataset.getRows().size());
                dataset.setCurrentRow(dataset.getRows().size());
            }
           
            resultSet.close();
        }
        catch(Exception e)
        {
            String errorInfo = "getSQLStyleDataviewDataset() failed! e="+e+",sql = "+dataview.getSqlStatement();
            log.error(errorInfo);
            throw new Exception(errorInfo);
        }
        finally
        {
            if ( stmt!=null )
                JDBCUtil.close(stmt);     
            
            if ( conn!=null )
                JDBCUtil.close(conn);
        }
        
        return dataset;
    }
    
    public static Dataset getStoreProcedureDataviewDataset(AnalyticDataview dataview, EntityManager platformEm,EntityManager em,String storeProcedureParameterValues) throws Exception
    {
        DatasourceConnection datasourceConnection = null;
        Connection conn = null;
        ResultSet resultSet = null;
        Dataset dataset = new Dataset();
        DatasetInfo datasetInfo;
        int maxSize;
        String storeProcedureSql = "";
        CallableStatement stmt = null;  
        List<Integer> outParameterIds = new ArrayList<>();
        List<String> allStoreProcedureParameters = new ArrayList<>();
        String resultStr = "";
        List<String> columnNames = new ArrayList<>();
        Map<String,Map<String,String>> columnInfoMap = new HashMap<>();
        String[] parameterVals = new String[]{};
         
        try
        {                
            datasourceConnection = Util.getDatasourceConnectionById(em, dataview.getOrganizationId(), dataview.getRepositoryId());
            
            conn = JDBCUtil.getJdbcConnection(datasourceConnection);
     
            storeProcedureSql = dataview.getSqlStatement();
                             
            if ( storeProcedureParameterValues != null && !storeProcedureParameterValues.trim().isEmpty() )
                parameterVals = storeProcedureParameterValues.split("\\,");
            
            allStoreProcedureParameters = DataviewUtil.extraceStoreProcedureParameterStrs(dataview.getSqlStatement(),"");
                      
            for(String val: allStoreProcedureParameters)
                storeProcedureSql = storeProcedureSql.replaceAll(val, ""); 
           
            log.info("getStoreProcedureDataviewDataset excuting sql="+storeProcedureSql);
                        
            stmt = conn.prepareCall(storeProcedureSql);

            int i = 0;
            for(String para : allStoreProcedureParameters)
            {
                i++;

                if ( para.toUpperCase().startsWith("IN") )
                {
                    String paraValue = "";
                    
                    for(String val : parameterVals)
                    {
                        if ( val.startsWith(para) )
                        {
                            paraValue = val;
                            break;
                        }
                    }
 
                    if ( paraValue.isEmpty() )
                    {
                        if ( para.toUpperCase().contains("IN_TODAY_") )  // ?IN_CURRENT_TIME_yyyyMMdd_CHAR
                        {
                            String datetimeFormat = para.substring("IN_TODAY_".length()); log.info("33333 datetimeformat="+datetimeFormat);
                            paraValue = String.format("%s`%s",para,Tool.convertDateToString(new Date(),datetimeFormat));
                        }  
                        else
                        if ( para.toUpperCase().contains("IN_YESTERDAY_") )  // ?IN_CURRENT_TIME_yyyyMMdd_CHAR
                        {
                            String datetimeFormat = para.substring("IN_YESTERDAY_".length()); log.info("33333 datetimeformat="+datetimeFormat);
                            paraValue = String.format("%s`%s",para,Tool.convertDateToString(Tool.dateAddDiff(new Date(),-24,Calendar.HOUR),datetimeFormat));
                        }  
                    }
                    
                    if ( paraValue.isEmpty() )
                        throw new Exception("parameter has no value! parameter="+para);
                    
                    String[] vals1 = paraValue.split("\\`");

                    if ( vals1[0].toUpperCase().contains("INTEGER") )
                        stmt.setInt(i, Integer.parseInt(vals1[1]));
                    else
                    if ( vals1[0].toUpperCase().contains("CHAR") )
                        stmt.setString(i, vals1[1]);
                    else
                    if ( vals1[0].toUpperCase().contains("DECIMAL") )
                        stmt.setDouble(i, Double.parseDouble(vals1[1]));
                }
                else
                {
                    if ( para.toUpperCase().contains("INTEGER") )
                        stmt.registerOutParameter(i, Types.INTEGER); 
                    else
                    if ( para.toUpperCase().contains("CHAR") )
                        stmt.registerOutParameter(i, Types.VARCHAR); 
                    else
                    if ( para.toUpperCase().contains("DECIMAL") )
                        stmt.registerOutParameter(i, Types.DECIMAL); 
                                        
                    outParameterIds.add(i-1);
                }
            }
               
            if ( !outParameterIds.isEmpty() )  // has out parameters
            {
                stmt.execute();
                                     
                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);

                datasetInfo.setDatasetName(dataview.getName());
                datasetInfo.setDatasetSourceType(DatasetSourceType.DATAVIEW.getValue());
                datasetInfo.setDatasetSource(dataview.getName());                    
                
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);

                dataset.setRows(new ArrayList<Map<String,String>>());
                  
                Map<String,String> map = new HashMap<>();
                
                for(Integer id : outParameterIds)
                {
                    String paraName = allStoreProcedureParameters.get(id);
                    
                    columnNames.add(paraName);
                    
                    Map<String,String> columnInfo = new HashMap<>();

                    columnInfo.put("description",paraName);
                    columnInfo.put("precision","0");
                        
                    if ( paraName.toUpperCase().contains("INTEGER") )
                    {
                       resultStr = String.valueOf(stmt.getInt(id+1));  
                       columnInfo.put("type", String.valueOf(MetadataDataType.INTEGER.getValue()));
                       columnInfo.put("len", String.valueOf(MetadataDataType.INTEGER.getDefaultLength()));
                    }
                    else
                    if ( paraName.toUpperCase().contains("CHAR") )
                    {
                        resultStr = String.valueOf(stmt.getString(id+1));  
                        columnInfo.put("type", String.valueOf(MetadataDataType.STRING.getValue()));
                        columnInfo.put("len", "50");
                    }
                    else
                    if ( paraName.toUpperCase().contains("DECIMAL") )
                    {
                        resultStr = String.valueOf(stmt.getDouble(id+1));
                        columnInfo.put("type", String.valueOf(MetadataDataType.DOUBLE.getValue()));
                        columnInfo.put("len", String.valueOf(MetadataDataType.DOUBLE.getDefaultLength()));
                    }
                                        
                    map.put(paraName, resultStr);
                    columnInfoMap.put(paraName, columnInfo);
                }
                
                dataset.getRows().add(map);
            }
            else
            {
                resultSet = stmt.executeQuery(); 

                if ( dataview.getRowSelectType() == RowSelectType.RANGE.getValue() )
                    maxSize = dataview.getStartRow()+dataview.getExpectedRowNumber()-1;
                else
                    maxSize = dataview.getExpectedRowNumber();
                
                try
                {
                    if ( resultSet.next() == true )
                    {
                        ResultSetMetaData rsmd = resultSet.getMetaData();
                        for(int j=1;j<=rsmd.getColumnCount();j++)
                        {
                            String columnName = rsmd.getColumnName(j);
                            int len = rsmd.getColumnDisplaySize(j);
                            int precision =  rsmd.getScale(j);
                            int type = Util.convertToSystemDataType(String.valueOf(rsmd.getColumnType(j)),String.valueOf(len),String.valueOf(precision));

                            columnNames.add(columnName);

                            Map<String,String> columnInfo = new HashMap<>();

                            columnInfo.put("type", String.valueOf(type));
                            columnInfo.put("description",columnName);
                            columnInfo.put("len", String.valueOf(len));
                            columnInfo.put("precision",String.valueOf(precision));

                            columnInfoMap.put(columnName, columnInfo);
                        }

                        datasetInfo = new DatasetInfo();
                        dataset.setDatasetInfo(datasetInfo);

                        datasetInfo.setDatasetName(dataview.getName());
                        datasetInfo.setDatasetSourceType(DatasetSourceType.DATAVIEW.getValue());
                        datasetInfo.setDatasetSource(dataview.getName());                    

                        datasetInfo.setColumnNames(columnNames);
                        datasetInfo.setColumnInfo(columnInfoMap);

                        dataset.setRows(new ArrayList<Map<String,String>>());

                        int k=1;

                        do
                        {
                            if ( dataview.getRowSelectType() == RowSelectType.RANGE.getValue() )
                            {
                                if ( k < dataview.getStartRow() )
                                {
                                    k++;
                                    continue;
                                }
                            }

                            if ( k > maxSize && maxSize > 0 )
                            {
                                if ( resultSet.next() == false )
                                    break;

                                k++;
                                continue;
                            }

                            Map<String,String> map = new HashMap<>();

                            for(String cName : columnNames)
                            {
                                String columnValue = resultSet.getString(cName);

                                if ( columnValue == null )
                                    columnValue = "";

                                map.put(cName, columnValue);
                            }

                            dataset.getRows().add(map);

                            k++;
                        }
                        while(resultSet.next());

                        dataset.setTotalRow(k-1);
                        dataset.setCurrentRow(dataset.getRows().size());
                    }                
                }
                catch(Exception e)
                {
                    log.info("read from resultset failed! e="+e+",stacktrace="+ExceptionUtils.getStackTrace(e));
                }                
                                           
                resultSet.close();
            }
        }
        catch(Exception e)
        {
            String errorInfo = "getSQLStyleDataviewDataset() failed! e="+e+", sql = "+dataview.getSqlStatement()+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
            throw new Exception(errorInfo);
        }
        finally
        {
            if ( stmt!=null )
                JDBCUtil.close(stmt);     
            
            if ( conn!=null )
                JDBCUtil.close(conn);
        }
        
        return dataset;
    }
     
    private static String getStoreProcedureSql(String sqlStatement,String storeProcedureParameterValues)
    {
        String[] vals = storeProcedureParameterValues.split("\\,");
        
        for(String val: vals)
        {
            String[] vals1=val.split("\\`");
            sqlStatement = sqlStatement.replaceAll(vals1[0], vals1[1]); 
        }
        
        return sqlStatement;
    }
   
    public static void addAssociatedDataToDataview(Dataset dataset, AnalyticDataview dataview, EntityManager em, EntityManager platformEm,boolean needDatasetInfo) throws Exception
    {
        String[] indexTypes;
        String[] indexNames = new String[]{};
        DataobjectType slaveDataobjectType;
        Map<String,Object> ret;
        boolean isPrimaryGroupHit;
        List<String> columns;
        String[] columnMetadataGroups;
        
        log.info(" addAssciatedData 11111111");
        
        int thisDataobjectType = dataview.getDataobjectTypeId();
                
        List<Map<String,String>> workingRows = new ArrayList<>();
        String adtcStr = dataview.getAssociatedDataobjectTypeColumns();
        
        if ( adtcStr == null || adtcStr.trim().isEmpty() )
            return;
        
        Client esClient = ESUtil.getClient(em,platformEm,dataview.getRepositoryId(),false);
          
        String[] associatedDataobjectTypes = adtcStr.split("\\;");
        
        for(String associatedDataobjectType : associatedDataobjectTypes )
        {
            log.info("333333333333 process associatedDataobject type="+ associatedDataobjectType);
            
            String[] vals = associatedDataobjectType.split("\\,");
      
            int associatedDataobjectTypeId = Integer.parseInt(vals[0]);
            slaveDataobjectType = platformEm.find(DataobjectType.class, associatedDataobjectTypeId);
                                                  
            DataobjectTypeAssociation dta = Util.getDataobjectTypeAssociation(platformEm, dataview.getOrganizationId(), thisDataobjectType, associatedDataobjectTypeId);

            if ( dta == null )
            {
                log.info(" no dta : thisDataobjectType="+thisDataobjectType+" associatedDataobjectTypeId="+associatedDataobjectTypeId);
                continue;
            }
                               
            columns = new ArrayList<>();
                  
            for(int i=1;i<vals.length;i++)
                columns.add(vals[i]);
            
            columnMetadataGroups = getColumnMetadataGroups(slaveDataobjectType,columns.toArray(new String[1]));
            
            if ( needDatasetInfo )
                addAssociatedColumnToDatasetInfo(dataset.getDatasetInfo(),slaveDataobjectType,columns);
                       
            int i = 0;
            int j = 0;
            
            for(Map<String,String> row : dataset.getRows())
            {
                i++;
                j++;
                
                if ( i == 1 )
                    workingRows = new ArrayList<>();
                                        
                if ( i <= 950 )
                {
                    workingRows.add(row);
                    
                    if ( j < dataset.getRows().size() )
                        continue;
                }
                         
                if ( dta.getMasterDataobjectTypeId() == thisDataobjectType )
                    ret = Util.getMutipleSlaveFilterStrFromRowNew(workingRows, dta);
                else
                    ret = Util.getMutipleMasterFilterStrFromRowNew(workingRows, dta);
                
                String[] masterColumnNames = ((List<String>)ret.get("masterColumnNames")).toArray(new String[1]);
                String[] slaveColumnNames = ((List<String>)ret.get("slaveColumnNames")).toArray(new String[1]);
                List<String> slaveColumnNameList = (List<String>)ret.get("slaveColumnNames");
                slaveColumnNameList.addAll(columns);
                
                String[] slaveColumnMetadataGroups = getColumnMetadataGroups(slaveDataobjectType,slaveColumnNames);

                String filterStr = (String)ret.get("filterStr");
                String flag = (String)ret.get("flag");
                
                log.info("555555555555555 flag="+flag);

                indexTypes = new String[]{slaveDataobjectType.getIndexTypeName().trim()};

                List<Map<String,Object>> searchResults = new ArrayList<>();
                
                try
                {
                    Date date1 = new Date();
                    
                    log.info("3333333333333333333 search filterstr="+Tool.getStringWithMaxLen(filterStr,100));
                    Map<String,Object> searchRet = ESUtil.retrieveAllDataset(esClient, indexNames, indexTypes, "", filterStr, slaveColumnNameList.toArray(new String[1]), null, Integer.MAX_VALUE,null);
                    searchResults = (List<Map<String,Object>>)searchRet.get("searchResults");
                    
                    log.info("4444444444444444 finish search took "+(new Date().getTime() - date1.getTime())+" ms");
                }
                catch(Exception e)
                {
                    log.error("11111111111111 retriveAllDataset Failed! ");
                }                

                if ( searchResults.isEmpty() )
                {
                    i = 0;
                    continue;
                }
                
                for(int kk=0;kk<workingRows.size();kk++)
                {
                    Map<String,String> currentRow = workingRows.get(kk);
                    
                    String[] masterColumnValues = new String[masterColumnNames.length];

                    for(int k=0;k<masterColumnNames.length;k++)
                        masterColumnValues[k]=currentRow.get(masterColumnNames[k]);

                    for(Map<String,Object> result:searchResults)
                    {
                        JSONObject resultFields = new JSONObject((String)result.get("fields"));

                        //key = resultFields.optString(slaveColumnName);
                        String[] slaveColumnValues = new String[slaveColumnNames.length];

                        for(int k=0;k<slaveColumnNames.length;k++)
                            slaveColumnValues[k]=resultFields.optString(slaveColumnNames[k]);

                        if ( flag.equals(" AND ") )
                        {
                            boolean same = true;

                            for(int k=0;k<masterColumnNames.length;k++)
                            {
                                if ( !masterColumnValues[k].equals(slaveColumnValues[k]) )
                                {
                                    same = false;
                                    break;
                                }
                            }

                            if ( same )
                            {
                                for(String columnName : columns)
                                {
                                    currentRow.put(columnName, resultFields.optString(columnName));
                                }

                                break;
                            }
                        }
                        else
                        {
                            boolean same = false;
                            isPrimaryGroupHit = true;

                            for(int k=0;k<masterColumnNames.length;k++)
                            {
                                if ( masterColumnValues[k].equals(slaveColumnValues[k]) )
                                {
                                    same = true;
                                    
                                    if ( slaveColumnMetadataGroups[k].equals("1") )
                                        isPrimaryGroupHit = true;
                                    else
                                        isPrimaryGroupHit = false;
                                        
                                    break;
                                }
                            }

                            if ( same )
                            {
                                int kkk = 0;
                                
                                for(String columnName : columns)
                                {
                                    if ( isPrimaryGroupHit )
                                        currentRow.put(columnName, resultFields.optString(columnName));
                                    else
                                    {
                                        if ( columnMetadataGroups[kkk].equals("0") )
                                        {
                                            currentRow.put(columnName, resultFields.optString(columnName));
                                        }
                                    }
                                    
                                    kkk++;
                                }

                                break;
                            }
                        }
                    }
                }
                
                i=0;
            }
        }
    }
     
    private static void addAssociatedColumnToDatasetInfo(DatasetInfo datasetInfo,DataobjectType associatedDataobjectType,List<String>columns) throws Exception
    {
        Map<String,Map<String,String>> columnInfoMap = datasetInfo.getColumnInfo();
        List<Map<String,Object>> metadataDefintion = new ArrayList<>();
        Util.getSingleDataobjectTypeMetadataDefinition(metadataDefintion,associatedDataobjectType.getMetadatas());
        
        for(String cName : columns)
        {            
            for(Map<String,Object> definition : metadataDefintion)
            {
                String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);

                if ( !cName.equals(columnName) )
                    continue;

                String description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                String type = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                String len = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);

                Map<String,String> columnInfo = new HashMap<>();
                columnInfo.put("type", type);
                columnInfo.put("description",description);
                columnInfo.put("len", len);

                columnInfoMap.put(columnName, columnInfo);
                
                break;
            }
        }
     
        datasetInfo.getColumnNames().addAll(columns);
    }
    
    private static String[] getColumnMetadataGroups(DataobjectType associatedDataobjectType,String[] columns) throws Exception
    {
        String[] groups = new String[columns.length];
        
        List<Map<String,Object>> metadataDefintion = new ArrayList<>();
        Util.getSingleDataobjectTypeMetadataDefinition(metadataDefintion,associatedDataobjectType.getMetadatas());
        
        int i=0;
        
        for(String cName : columns)
        {            
            for(Map<String,Object> definition : metadataDefintion)
            {
                String columnName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);

                if ( !cName.equals(columnName) )
                    continue;
 
                Integer metadataGroup = (Integer)definition.get(CommonKeys.METADATA_NODE_METADATA_GROUP);
                 
                groups[i] = String.valueOf(metadataGroup);
                i++;
                
                break;
            }
        }
         
        return groups;
    }
    
    public static void selectOutputColumnsAndOrder(Dataset dataset, AnalyticDataview dataview, EntityManager platformEm)
    {
         List<String> newColumnNames = new ArrayList<>();
        
         String str = dataview.getOutputColumns();
        
        if ( str == null || str.trim().isEmpty() )
            return;
        
        String [] vals = str.split("\\;");
        
        for(String val : vals)
        {
            String [] nextVals = val.split("\\,");
            
           // newColumnNames.add(nextVals[0]);
            
            if ( !nextVals[0].contains("-") )
                newColumnNames.add(nextVals[0]);
            else
                newColumnNames.add(nextVals[0].substring(0, nextVals[0].indexOf("-")));
        }
 
        dataset.getDatasetInfo().setColumnNames(newColumnNames);
        
        Map<String,Map<String,String>> newColumnInfo = new HashMap<>();
        List<String> removedColumnNames = new ArrayList<>();
        
        for(  Map.Entry<String,Map<String,String>>  entry : dataset.getDatasetInfo().getColumnInfo().entrySet() )
        {                
            String cName = entry.getKey();
            
            boolean found = false;
            
            for(String newColumn : newColumnNames)
            {
                if ( newColumn.equals(cName) )
                {
                    found = true;
                    break;
                }
            }
 
            if ( found )
                newColumnInfo.put(cName, entry.getValue());        
            else
                removedColumnNames.add(cName);
        }
        
        if ( removedColumnNames.isEmpty() )
            return;
                
        dataset.getDatasetInfo().setColumnInfo(newColumnInfo);
                        
        for(Map<String,String> row : dataset.getRows())
        {
            for(String removedColumnName : removedColumnNames)
                row.remove(removedColumnName);
        }
    }
    
    public static void processAddNewColumn(EntityManager em,Dataset dataset, AnalyticDataview dataview, boolean needDatasetInfo) throws IOException, ClassNotFoundException
    { 
        List<Map<String,Object>> newColumnMapList = (List<Map<String,Object>>)Tool.deserializeObject(dataview.getNewColumnData());
       
        if ( needDatasetInfo )
        {
            Map<String,Map<String,String>> columnInfoMap = dataset.getDatasetInfo().getColumnInfo();

            List<String> columns = new ArrayList<>();
            
            for(Map<String,Object> newColumnMap : newColumnMapList)
            {
                String columnName = (String)newColumnMap.get("columnName");
                String columnDescription = (String)newColumnMap.get("columnDescription");
                String dataType = (String)newColumnMap.get("dataType");
                String length = (String)newColumnMap.get("length");
 
                Map<String,String> columnInfo = new HashMap<>();
                columnInfo.put("type", dataType);
                columnInfo.put("description",columnDescription);
                columnInfo.put("len", length);

                columnInfoMap.put(columnName, columnInfo);
            
                columns.add(columnName);
            }

            dataset.getDatasetInfo().getColumnNames().addAll(columns);
        }

        for(Map<String,String> row : dataset.getRows())
        {            
            for(Map<String,Object> newColumnMap : newColumnMapList)
            {
                String columnName = (String)newColumnMap.get("columnName");

                String computingType = (String)newColumnMap.get("computingType");
                String sourceCodeColumn = (String)newColumnMap.get("sourceCodeColumn");
                String sourceCodeId = (String)newColumnMap.get("sourceCodeId");
                String targetCodeId = (String)newColumnMap.get("codeId");
           
                if ( Integer.parseInt(computingType) == NewColumnComputingType.FROM_CODE_COLUMN.getValue() )
                {         
                    String sourceCodeKey = row.get(sourceCodeColumn);
                 
                    String targetCodeKey = getNewCodeValue(em,sourceCodeId,sourceCodeKey,targetCodeId);
             
                    if ( targetCodeKey == null || targetCodeKey.trim().isEmpty() )
                        log.info("777777777777777777 targetCodeKey is null");
                    
                    row.put(columnName,targetCodeKey);
                }
            }
        }
    }
    
    private static String getNewCodeValue(EntityManager em,String sourceCodeId,String sourceCodeKey,String targetCodeId)
    {            
        if ( sourceCodeKey == null || sourceCodeKey.isEmpty() )
            return "";
        
        String sql = String.format("from CodeDetailsMapping where sourceCodeId=%s and sourceCodeKey=%s and targetCodeId=%s",sourceCodeId,sourceCodeKey,targetCodeId);
        List<CodeDetailsMapping> list = em.createQuery(sql).getResultList();
        
        if ( !list.isEmpty() )
            return list.get(0).getTargetCodeKey();
 
        sql = String.format("from CodeDetailsMapping where targetCodeId=%s and targetCodeKey=%s and sourceCodeId=%s",sourceCodeId,sourceCodeKey,targetCodeId);
        list = em.createQuery(sql).getResultList();
        
        if ( !list.isEmpty() )
            return list.get(0).getSourceCodeKey();
                    
        return "";
    }
}
      