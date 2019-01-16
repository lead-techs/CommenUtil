package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.ResultScanner; 
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.filter.Filter; 
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; 
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp; 

public class HBaseUtil 
{
    static final Logger log = Logger.getLogger("HBaseUtil");      

    public static Connection conn;
    public static String defaultFamilyName;
    public static String hbaseIp;
    public static String hbasePort;
    public static String znodeParent;
   
    public static void initConn(String hbaseIp1,String hbasePort1,String znodeParent1,String defaultFamilyName1) throws IOException 
    {
        Configuration config = HBaseConfiguration.create();
        
        config.set("hbase.zookeeper.quorum", hbaseIp1);
        config.set("hbase.zookeeper.property.clientPort", hbasePort1);
        config.set("zookeeper.znode.parent", znodeParent1);
        
        hbaseIp = hbaseIp1;
        hbasePort = hbasePort1;
        znodeParent = znodeParent1;
        defaultFamilyName = defaultFamilyName1;
        
        int retry = 0;
        while(true)
        {
            retry++;
            
            try
            {
                log.info("3333 connecting to hbase ... ip="+hbaseIp1);
        
                conn = ConnectionFactory.createConnection(config); 
                
                log.info("4444 hbase conn ="+conn);
               
                if ( conn == null )
                    continue;
               
                break;
            } 
            catch (IOException e) 
            {
               log.error("5555 init hbase connection failed! e="+e);
               
               if ( retry > 10)
                   throw e;
               
               Tool.SleepAWhile(1, 0);
            }
        }
    }
    
    public static void reInit()
    {
        Configuration config = HBaseConfiguration.create();
        
        config.set("hbase.zookeeper.quorum", hbaseIp);
        config.set("hbase.zookeeper.property.clientPort", hbasePort);
        config.set("zookeeper.znode.parent", znodeParent);

        try 
        {
            log.info("111 reinit hbase .... ip="+hbaseIp+" port="+hbasePort+" znodeParent="+znodeParent);
            
           conn = ConnectionFactory.createConnection(config); 
           
           log.info("22222222222222222222 hbase conn ="+conn);
        } 
        catch (Exception e) {
           log.error(" init hbase connection failed! e="+e);
        }
    }
    
    public static void closeConn()
    {
        try 
        {
           conn.close();
        } 
        catch (Exception e) {
           log.error(" init hbase connection failed! e="+e);
        }
    }
    
    public static List<Map<String,Object>> getTableColumnInfo(String hbaseIp,String hbasePort,String znodeParent,String tableName,String familyName) throws IOException, Exception
    {
        Map<String,Object> columnInfo;
        List<Map<String,Object>> columnInfoList = new ArrayList<>();
        
        try 
        {
            initConn(hbaseIp,hbasePort,znodeParent,familyName);
        
            String sqlStr = String.format("select * from %s.%s limit 1",tableName,familyName);
            Map<String,Object> ret = getRowDataBySQL(sqlStr,1); 
            
            List<Map<String,String>> rows = (List<Map<String,String>>)ret.get("rows");
            
            if ( rows != null && rows.size() > 0 )
            {
                for(Map.Entry<String,String> entry : rows.get(0).entrySet())
                {                    
                    columnInfo = new HashMap<>();
                    columnInfo.put("isPrimaryKey", false);
                    columnInfo.put("columnName", entry.getKey());
                    columnInfo.put("jdbcDataType", "1");
                    columnInfo.put("typeName", "String");                 
                    columnInfo.put("description", ""); 
                    columnInfo.put("length", entry.getValue().length()); 
                    columnInfo.put("precision","0");
 
                    columnInfoList.add(columnInfo);
                }
            }
            
            return columnInfoList;
        } 
        catch(Exception e)
        {
            log.error(" getAllTableColumnName() failed! e="+e);
            throw e;
        }
        finally {
           closeConn();
        }
    }
    
    public static List<String> getAllTableNames(String hbaseIp,String hbasePort,String znodeParent,String filter) throws IOException
    {
        try
        {
            initConn(hbaseIp,hbasePort,znodeParent,null);
        
            List<String> list = getAllTableNames();
            List<String> newList = new ArrayList<>();
            
            for(String name : list)
            {
                if ( !name.startsWith(filter) )
                    newList.add(name);
            }
            
            return newList;
        }
        catch(IOException e)
        {
            log.error(" getAllTableNames() failed! e="+e);
            throw e;
        }
        finally {
           closeConn();
        }
    }
        
    public static List<String> getAllTableNames() throws IOException
    {
        Admin admin = null;
        List<String> tableNames = new ArrayList<>();
        
        try 
        {
           admin = conn.getAdmin();
           HTableDescriptor[] tableDescriptor = admin.listTables();
           
           for(HTableDescriptor descriptor : tableDescriptor)           
               tableNames.add(descriptor.getNameAsString());
           
           return tableNames;
        } 
        catch(IOException e)
        {
            log.error(" getAllTableNames() failed! e="+e);
            throw e;
        }
        finally {
           IOUtils.closeQuietly(admin);
        }
    }
    
    public static boolean checkTableExists(String tableName) throws IOException
    {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        
        try 
        {
           admin = conn.getAdmin();
           return admin.tableExists(table);
        } 
        catch(IOException e)
        {
            log.error(" checkTableExists failed! e="+e);
            throw e;
        }
        finally {
           IOUtils.closeQuietly(admin);
        }
    }

    //创建表
    public static void createTable(String tableName,String familyName) throws IllegalArgumentException, IOException 
    {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        
        try 
        {
           log.info(" creating table "+tableName);
            
            admin = conn.getAdmin();
            if (!admin.tableExists(table)) 
            {
                HTableDescriptor descriptor = new HTableDescriptor(table);
              
                if ( familyName != null && !familyName.trim().isEmpty() )
                {
                    String[] families = familyName.split("\\.");
              
                    for (String s : families) {
                        descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                    }
                }
                else
                {
                    descriptor.addFamily(new HColumnDescriptor(defaultFamilyName.getBytes()));
                }
              
                admin.createTable(descriptor);
            }
        }
        catch(IOException e)
        {
            log.error(" createTable failed! e="+e);
            throw e;
        }
        finally {
           IOUtils.closeQuietly(admin);
        }
    }
    
        
    //删除表
    public static void dropTable(String tableName) throws IOException 
    {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        
        try 
        {
            admin = conn.getAdmin();
            
            if (admin.tableExists(table)) 
            {
                admin.disableTable(table);
                admin.deleteTable(table);
            }
        } 
        catch(IOException e)
        {
            log.error(" dropTable() failed! e="+e);
            throw e;
        }
        finally {
           IOUtils.closeQuietly(admin);
        }
    }
    
    //添加数据
    public static void addData(String tableName,String familyName,String rowKey, Map<String,String> columns) throws IOException  
    {
        Table table = null;
        int retry = 0;
        
        while(true)
        {
            retry++;
              
            try 
            {
                table = conn.getTable(TableName.valueOf(tableName));
                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map.Entry<String,String> entry : columns.entrySet()) 
                {
                    put.addColumn(familyName.getBytes(), Bytes.toBytes(entry.getKey()),
                         Bytes.toBytes((String)(entry.getValue())));
                }

                table.put(put);
                
                break;
            }
            catch(IOException e)
            {
                log.error("addData() failed! e="+e);

                if ( e.toString().contains("Table") && e.toString().contains("not found") )
                {
                    createTable(tableName,familyName);
                    continue;
                }
                
                if ( retry > 100 )
                    throw e;
                
                reInit();
            }
            finally {
               IOUtils.closeQuietly(table);
            }
        }
              
      
    }
        
    //添加批量数据
    public static void addBatchData(String tableName,String familyName,Map<String, Map<String,String>> rows) throws IOException  
    {
        String rowKey;
        Map<String, String> columns;
                                
        Table table = null;
        List<Put> putList = new ArrayList<>();
        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try
            {
                table = conn.getTable(TableName.valueOf(tableName));

                for( Map.Entry<String, Map<String,String>> rowEntry : rows.entrySet() ) 
                {
                    rowKey = rowEntry.getKey();
                    columns = rowEntry.getValue();

                    Put put = new Put(Bytes.toBytes(rowKey));
                    for (Map.Entry<String, String> columnEntry : columns.entrySet()) 
                    {
                        put.addColumn(familyName.getBytes(), Bytes.toBytes(columnEntry.getKey()),
                        Bytes.toBytes((String)(columnEntry.getValue())));
                    }

                    putList.add(put);
                }

                table.put(putList);
                break;
            }
            catch(Exception e)
            {
                log.error("addBatchData() failed! e="+e+" stack trace="+ExceptionUtils.getStackTrace(e));
                
                if ( e.toString().contains("Table") && e.toString().contains("not found") )
                {
                    createTable(tableName,familyName);
                    continue;
                }
                
                if ( retry > 10 )
                    throw e;
                
                reInit();
            }
            finally {
               IOUtils.closeQuietly(table);
            }
        }
       
    }
     
    
    //根据rowkey获取数据
    public static Map<String, String> getRowData(String tableName,String familyName,String rowKey) throws IllegalArgumentException, IOException 
    {
        Table table = null;
        Map<String, String> resultMap = null;
        
        try 
        {
           table = conn.getTable(TableName.valueOf(tableName));
           Get get = new Get(Bytes.toBytes(rowKey));
           get.addFamily(familyName.getBytes());
           Result res = table.get(get);
           Map<byte[], byte[]> result = res.getFamilyMap(familyName.getBytes());
           
           Iterator<Entry<byte[], byte[]>> it = result.entrySet().iterator();
           resultMap = new HashMap<>();
           
           while (it.hasNext()) {
              Entry<byte[], byte[]> entry = it.next();
              resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
           }
        } 
        catch(IOException e)
        {
            log.error("getRowAllColumnData() failed! e="+e);
            throw e;
        }
        finally {
           IOUtils.closeQuietly(table);
        }
        return resultMap;
    }

    //根据rowkey和column获取数据
    public static String getRowOneColumnData(String tableName,String familyName,String rowKey,String column) throws IllegalArgumentException, IOException 
    {
        Table table = null;
        String resultStr = null;
        
        try 
        {
             table = conn.getTable(TableName.valueOf(tableName));
             Get get = new Get(Bytes.toBytes(rowKey));
             get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column));
             Result res = table.get(get);
             byte[] result = res.getValue(Bytes.toBytes(familyName), Bytes.toBytes(column));
             resultStr = Bytes.toString(result);
        } 
        catch(IOException e)
        {
            log.error("getRowOneColumnData() failed! e="+e);
            throw e;
        }
        finally {
           IOUtils.closeQuietly(table);
        }
        
        return resultStr;
    }

    //批量查询数据
    public static List<Map<String,String>> getMutipleRowData(String tableName,String familyName,List<String> rowKeys,List<String> filterColumn) throws IOException
    {
        List<Map<String,String>> rows = new ArrayList<>();
        
        Table table = null;
        List<Get> listGets = new ArrayList<>();
        
        for (String rk : rowKeys)
        {
            Get get = new Get(Bytes.toBytes(rk));
            if (filterColumn != null)
            {
                for (String column : filterColumn)
                    get.addColumn(familyName.getBytes(), column.getBytes());
            }
            
            listGets.add(get);
        }
        
        try
        {
            table = conn.getTable(TableName.valueOf(tableName));
            Result[] results = table.get(listGets);
                        
            for(Result result : results)
            {
                Map<byte[], byte[]> resultMap = result.getFamilyMap(familyName.getBytes());
           
                Iterator<Entry<byte[], byte[]>> it = resultMap.entrySet().iterator();
                Map<String,String> map = new HashMap<>();
                        
                while (it.hasNext()) {
                    Entry<byte[], byte[]> entry = it.next();
                    map.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
                }
                             
                rows.add(map);
            } 
        }
        catch (IOException e)
        {
            log.error(" getMutipleRowData() failed! e="+e);
            throw e;
        }
        finally {
           IOUtils.closeQuietly(table);
        }
        
        return rows;
    }
    
    public static Map<String,Object> getRowDataByConditions(SimpleSQLSturecture sqlStructure) throws Exception 
    { 
        boolean onlyCheckCount = false;
        List<Map<String,String>> rows = new ArrayList<>();
        String familyName = null;
        List<String> columns = null;
        Map<String,Object> ret = new HashMap<>();
        
        try 
        {            
            if ( sqlStructure.familiesStr == null || sqlStructure.familiesStr.trim().isEmpty() )
                familyName = defaultFamilyName;
            else
                familyName = sqlStructure.familiesStr;
 
            if ( sqlStructure.selectFieldNames == null || sqlStructure.selectFieldNames.isEmpty() )
                columns = getTableColumnNames(sqlStructure.tableName,sqlStructure.familiesStr);
            else
                columns = sqlStructure.selectFieldNames;
            
            Scan scan = new Scan(); 
            
            scan.setMaxResultSize(sqlStructure.maxExpected+sqlStructure.startFrom);
            
            if ( sqlStructure.startRow != null && !sqlStructure.startRow.trim().isEmpty() )
                scan.setStartRow(Bytes.toBytes(sqlStructure.startRow));
            
            if ( sqlStructure.stopRow != null && !sqlStructure.stopRow.trim().isEmpty() )
                scan.setStopRow(Bytes.toBytes(sqlStructure.stopRow));
                                    
            for(String column : columns)  
            {              
                if ( column.equals("count(*)") )
                    continue;
                
                scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column)); 
            }
            
            List<Filter> filters = new ArrayList<>(); 
   
            if ( sqlStructure.where != null )
            {
                for(Map.Entry<String,Map<Object,String>> map : sqlStructure.where.entrySet())
                {
                    String columnName = map.getKey();
                    Map<Object,String> condition = map.getValue();

                    condition.entrySet().iterator().next();

                    Map.Entry<Object,String> entry = condition.entrySet().iterator().next();
                    CompareOp comparaOp = (CompareOp)entry.getKey();
                    String value=entry.getValue();
                    
                    Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName),Bytes.toBytes(columnName),comparaOp,Bytes.toBytes(value)); 
                    filters.add(filter); 
                    
                    scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
                }
            }
        
            Table table = conn.getTable(TableName.valueOf(sqlStructure.tableName));
                     
            if ( filters.size() > 0 )
            {     
                FilterList filterList = new FilterList(filters); 
                scan.setFilter(filterList);
            }
            
            ResultScanner results = table.getScanner(scan); 
            
            long i = 0;
            long j = 0;
            
            if ( columns != null && columns.size() == 1)
            {
                if ( columns.get(0).equals("count(*)") )
                    onlyCheckCount = true;
            }
                
            long count = 0;
            
            for (Result result : results) 
            {
                count++;
                
                if ( count > sqlStructure.startFrom+sqlStructure.maxExpected )
                    break;
                
                if ( onlyCheckCount )
                    continue;
                
                if ( i < sqlStructure.startFrom )
                {
                    i++;
                    continue;
                }
                
                j++;
                                                
                if ( j > sqlStructure.maxExpectedFromClient && sqlStructure.maxExpectedFromClient>0 )
                    continue;
                
                Map<byte[], byte[]> resultMap = result.getFamilyMap(familyName.getBytes());
           
                Iterator<Entry<byte[], byte[]>> it = resultMap.entrySet().iterator();
               
                Map<String,String> map = new HashMap<>();
                        
                while (it.hasNext()) 
                {
                    Entry<byte[], byte[]> entry = it.next();
                    
                    String columnName = Bytes.toString(entry.getKey());
                    
                    if ( columns != null && columns.size()>0 )
                    {
                        for(String column : columns )
                        {
                            if ( columnName.equals(column) )
                                map.put(columnName, Bytes.toString(entry.getValue()));
                        }
                    }
                    else
                        map.put(columnName,Bytes.toString(entry.getValue()));
                }
                             
                rows.add(map);
            } 
            
            results.close();             
             
            if ( onlyCheckCount )
            {
                Map<String,String> map = new HashMap<>();
                map.put("count(*)",String.valueOf(count));
                rows.add(map);
            }
                        
            ret.put("count", j);
            ret.put("rows", rows);
            
            return ret;
 
        } catch (Exception e) 
        { 
            log.error("getRowDataByConditions() failed! e="+e);
            throw e;
        } 
    } 
    
    public static List<String> getTableColumnNames(String tableName,String familyName) throws Exception 
    { 
        List<String> columnNames = new ArrayList<>();
 
        try 
        {            
            if ( familyName == null || familyName.trim().isEmpty() )
                familyName = defaultFamilyName;
                         
            Scan scan = new Scan(); 
         
            Table table = conn.getTable(TableName.valueOf(tableName));   
       
            ResultScanner results = table.getScanner(scan); 
                    
            for (Result result : results) 
            {                 
                Map<byte[], byte[]> resultMap = result.getFamilyMap(familyName.getBytes());
           
                Iterator<Entry<byte[], byte[]>> it = resultMap.entrySet().iterator();
               
                Map<String,String> map = new HashMap<>();
                        
                while (it.hasNext()) 
                {
                    Entry<byte[], byte[]> entry = it.next();
                    
                    String columnName = Bytes.toString(entry.getKey());
                    columnNames.add(columnName);
                }

                break;
            } 
            
            results.close();             
                            
            return columnNames;
 
        } 
        catch (Exception e) 
        { 
            log.error("getTableColumnNames() failed! e="+e);
            throw e;
        } 
    } 
  
    public static Map<String,Object> getRowDataBySQL(String sqlStr,int maxExpectHitsFromClient) throws Exception 
    { 
        try 
        {
            SimpleSQLSturecture sqlStructure = convertSQLLikeStringToStructure(sqlStr);
            sqlStructure.setMaxExpectedFromClient(maxExpectHitsFromClient);
            
            return getRowDataByConditions(sqlStructure);
            
        } catch (Exception e) 
        { 
            log.error("getRowDataBySQL() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        } 
    } 
  
    public static SimpleSQLSturecture convertSQLLikeStringToStructure(String sqlLikeString) throws Exception
    {
        Map<Object,String> map;
        String[] vals;
        SimpleSQLSturecture sqlStructure = new SimpleSQLSturecture();

        String selectedColumnStr;
        String fromStr;
        String whereStr;
        String limitStr;
                          
        String sql = Util.preProcessSQLLikeString(sqlLikeString);
   
        if ( sql.contains("where"))
            fromStr = Tool.extractStr(sql,"from","where");
        else
        if ( sql.contains("limit"))
            fromStr = Tool.extractStr(sql,"from","limit");
        else
        if ( sql.contains("rowkey"))
            fromStr = Tool.extractStr(sql,"from","rowkey");
        else
            fromStr = Tool.extractStr(sql,"from","");

        vals = fromStr.split("\\.");
        
        if ( vals.length >1 )
        {
            sqlStructure.setTableName(vals[0]);
            sqlStructure.setFamiliesStr(fromStr.substring(fromStr.indexOf(".")+1));
        }
        else
            sqlStructure.setTableName(vals[0]);

        selectedColumnStr = Tool.extractStr(sql,"select","from");

        if ( selectedColumnStr.equals("*") )
            sqlStructure.setSelectFieldNames(new ArrayList<String>());
        else
        {
            vals = selectedColumnStr.split("\\;");
            sqlStructure.setSelectFieldNames(Arrays.asList(vals));
        }

        if ( sql.contains("where") )
        {
            if ( sql.contains("limit") )
                whereStr = Tool.extractStr(sql,"where","limit");
            else
            if ( sql.contains("rowkey"))
                whereStr = Tool.extractStr(sql,"where","rowkey");
            else
                whereStr = Tool.extractStr(sql,"where","");
                                  
            vals = whereStr.split(" and ");

            Map<String,Map<Object,String>> conditionMap = new HashMap<>();

            for(String condition : vals)
            {
                if ( condition.contains("<=") )
                {
                    String columnName = condition.substring(0, condition.indexOf("<="));
                    String val = condition.substring(condition.indexOf("<=")+2);
                    val = Tool.removeAroundAllQuotation(val);

                    map = new HashMap<>();
                    map.put(CompareOp.LESS_OR_EQUAL,val);
                    conditionMap.put(columnName, map);
                }
                else
                if ( condition.contains(">=") )
                {
                    String columnName = condition.substring(0, condition.indexOf(">="));
                    String val = condition.substring(condition.indexOf(">=")+2);
                    val = Tool.removeAroundAllQuotation(val);

                    map = new HashMap<>();
                    map.put(CompareOp.GREATER_OR_EQUAL,val);
                    conditionMap.put(columnName, map);
                }
                else
                if ( condition.contains("<") )
                {
                    String columnName = condition.substring(0, condition.indexOf("<"));
                    String val = condition.substring(condition.indexOf("<")+1);
                    val = Tool.removeAroundAllQuotation(val);

                    map = new HashMap<>();
                    map.put(CompareOp.LESS,val);
                    conditionMap.put(columnName, map);
                }
                else
                if ( condition.contains(">") )
                {
                    String columnName = condition.substring(0, condition.indexOf(">"));
                    String val = condition.substring(condition.indexOf(">")+1);
                    val = Tool.removeAroundAllQuotation(val);

                    map = new HashMap<>();
                    map.put(CompareOp.GREATER,val);
                    conditionMap.put(columnName, map);
                }
                else
                if ( condition.contains("!=") )
                {
                    String columnName = condition.substring(0, condition.indexOf("!="));
                    String val = condition.substring(condition.indexOf("!=")+2);
                    val = Tool.removeAroundAllQuotation(val);

                    map = new HashMap<>();
                    map.put(CompareOp.NOT_EQUAL,val);
                    conditionMap.put(columnName, map);
                }
                else
                if ( condition.contains("=") )
                {
                    String columnName = condition.substring(0, condition.indexOf("="));
                    String val = condition.substring(condition.indexOf("=")+1);
                    val = Tool.removeAroundAllQuotation(val);

                    map = new HashMap<>();
                    map.put(CompareOp.EQUAL,val);
                    conditionMap.put(columnName, map);
                }
                else
                    throw new Exception(" not support!");
            }

            sqlStructure.setWhere(conditionMap);
        }
                       
        if ( sql.contains("limit") )
        {
            if ( sql.contains("rowkey"))
                limitStr = Tool.extractStr(sql,"limit","rowkey");
            else
                limitStr = Tool.extractStr(sql,"limit","");

            if ( limitStr.contains(",") )
            {
                vals = limitStr.split("\\,");

                sqlStructure.setStartFrom(Long.parseLong(vals[0]));
                sqlStructure.setMaxExpected(Long.parseLong(vals[1]));
            }
            else
            {
                sqlStructure.setStartFrom((long)0);
                sqlStructure.setMaxExpected(Long.parseLong(limitStr));
            }
        }
            
        if ( sql.contains("rowkey") )
        {
            String rowkeyStr = Tool.extractStr(sql,"rowkey","");
    
            if ( rowkeyStr.contains(";") )
            {
                vals = rowkeyStr.split("\\;");

                sqlStructure.setStartRow(vals[0]);
                sqlStructure.setStopRow(vals[1]);
            }
            else
            {
                sqlStructure.setStartRow(rowkeyStr);
                sqlStructure.setStopRow(null);
            }
        }
                    
        return sqlStructure;
    }

}
