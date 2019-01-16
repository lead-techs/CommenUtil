/*
 * JDBCUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.dom4j.Document;
import org.dom4j.Element;
import java.io.ByteArrayInputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.persistence.EntityManager;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.dom4j.io.SAXReader;
import java.lang.reflect.Field;
import java.sql.Driver;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;

import com.jcraft.jsch.JSch;  
import com.jcraft.jsch.Session;  

import com.broaddata.common.model.enumeration.DatabaseItemType;
import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
  
public class JDBCUtil 
{      
    static final Logger log=Logger.getLogger("JDBCUtil");      
    
    private static final Lock lockForDBConn = new ReentrantLock();
    private static Map<String,Connection> dbConnMap = new HashMap<>(); // organizationId-repositoryId, conn
    private static Map<Connection,Driver> oracleDBConn = new HashMap<>();
    
    private static final Lock lockForDBConnPool = new ReentrantLock();
    private static Map<String,DataSource> dbConnPoolMap = new HashMap<>(); 
    
    private static final Map<Connection,Session> jschSessionMap = new HashMap<>();
    
    public static String libFilePath;
    
    public static int MAX_ACTIVE_CONNECTION = 50;
            
    public static String getSqlTypeName(int i) throws IllegalArgumentException, IllegalAccessException 
    {
        for (Field f : java.sql.Types.class.getFields()) {
             if (f.getType().equals(int.class)) {
                if (f.getInt(f.getType()) == i)
                    return f.getName();
             }
        }
        
        return null;
    }
        
    public static long getQueryCountOnly(String countQuerySQL,Connection dbConn) throws Exception
    {
        try 
        {
            log.info(" countQuerySQL ="+countQuerySQL);
            
            PreparedStatement statement = dbConn.prepareStatement(countQuerySQL);
            ResultSet resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();
 
            long count = resultSet.getInt(1);             
            
            JDBCUtil.close(resultSet);
            JDBCUtil.close(statement);
            return count;
        }
        catch(Exception e)
        {
            log.info("getQueryCount() failed! e="+e);
            throw e;
        }
    }
        
    public static List<String> getDatabaseStoredProcedureNameList(DatasourceConnection selectedDatasourceConnection,String storeProcedurePrefix)
    {
        Connection conn = null;
        ResultSet res = null;
        
        List<String> storedProcedureNameList = new ArrayList<>();
            
        try
        {
            conn = getJdbcConnection(selectedDatasourceConnection);         
                      
            String schema = JDBCUtil.getDatabaseConnectionSchema(selectedDatasourceConnection);
            
            if ( schema.equals("%") )
                schema = null;
            
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            res = databaseMetaData.getProcedures(null, schema, null);
 
            while (res.next())
            {
                String procedureName = res.getString("PROCEDURE_NAME");

                if ( !storeProcedurePrefix.isEmpty() && !procedureName.toLowerCase().startsWith(storeProcedurePrefix.toLowerCase()) )
                    continue;
                
                ResultSet rs = databaseMetaData.getProcedureColumns(null,schema,procedureName,"%");
     
                String procedure = procedureName+"(";
     
                while(rs.next())
                {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    String columnType = rs.getString("COLUMN_TYPE");
                    String length = rs.getString("LENGTH");
                    
                    String typeName = getSqlTypeName(Integer.parseInt(dataType));
                    if ( typeName == null )
                        typeName = "";
                    
                    String inoutStr="unknown";
                    
                    if ( columnType.equals("1") || columnType.equals("2") )
                        inoutStr = "in";
                    else
                    if ( columnType.equals("3") )
                        inoutStr = "inout";
                    else
                    if ( columnType.equals("4") )
                        inoutStr = "out";
                    
                    if ( length == null || length.equals("null") || length.isEmpty() )
                        procedure += String.format("?%s_%s_%s,",inoutStr,columnName,typeName.toLowerCase());
                    else
                        procedure += String.format("?%s_%s_%s_%s,",inoutStr,columnName,typeName.toLowerCase(),length);
                }
                
                
                if ( !procedure.endsWith("(") )
                    procedure = procedure.substring(0, procedure.length()-1);
                    
                procedure += ")";
                
                storedProcedureNameList.add(procedure);
            }
        }
        catch(Exception e) {
            log.error(" getDatabaseStoreProcedureNameList() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        finally
        {
            close(res);
            close(conn);
        }
        
        return storedProcedureNameList;
    }
        
    public static List<String> getDatabaseItemList(DatasourceConnection selectedDatasourceConnection,int databaseItemTypeId)
    {
        List<String> databaseItemList = new ArrayList<>();
            
        try
        {
            String databaseTypeStr = Util.getDatasourceConnectionProperty(selectedDatasourceConnection, "database_type");
                        
            String name = DatabaseItemType.findByValue(databaseItemTypeId).getType();

            String schema = JDBCUtil.getDatabaseConnectionSchema(selectedDatasourceConnection);
            
            if ( DatabaseType.findByValue(Integer.parseInt(databaseTypeStr)).getValue() == DatabaseType.HIVE.getValue() )
                databaseItemList = JDBCUtil.getSparkSQLDatabaseTableOrViewList(selectedDatasourceConnection,name);
            else
            if ( (DatabaseType.findByValue(Integer.parseInt(databaseTypeStr)).getValue() == DatabaseType.ORACLE.getValue() || 
                    DatabaseType.findByValue(Integer.parseInt(databaseTypeStr)).getValue() == DatabaseType.ORACLE_SERVICE.getValue()) && !schema.equals("%") )
            {
                databaseItemList = JDBCUtil.getOracleDatabaseTableOrViewList(selectedDatasourceConnection,schema,name);
            }
            else
                databaseItemList = JDBCUtil.getDatabaseTableOrViewList(selectedDatasourceConnection,name);
        }
        catch(Exception e) {
            log.error(" getDatabaseItemList() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        
        return databaseItemList;
    }
        
    public static List<String> getTableColumnNames(Connection dbConn,String schema,String tableName) throws SQLException
    {
        List<String> columnNames = new ArrayList<>();
        
        DatabaseMetaData databaseMetaData = dbConn.getMetaData(); 
               
        if ( schema.trim().isEmpty() || schema.equals("%") )
            schema = null;
        
        ResultSet columnSet = databaseMetaData.getColumns(null, schema, tableName, "%");

        while(columnSet.next())
        {
            columnNames.add(columnSet.getString("COLUMN_NAME"));
        }
            
        return columnNames;
    }
    
    public static List<String> getAddColumnSqlList(DatabaseType databaseType,String tableName,String columnName,String columnType,String columnComment)
    {
        List<String> addColumnSqlList = new ArrayList<>();
    
        switch(databaseType)
        {
            case GREENPLUM:    
                if ( columnType.equals("DATETIME") )
                    columnType = "TIMESTAMP";
                else
                if ( columnType.equals("DOUBLE") || columnType.equals("REAL") )
                    columnType = String.format("DECIMAL");
                
                addColumnSqlList.add(String.format("ALTER TABLE %s ADD %s %s",tableName,columnName,columnType));
                addColumnSqlList.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'",tableName,columnName,columnComment));
                break;
                
            case DB2:
            case ORACLE:
   
                 addColumnSqlList.add(String.format("ALTER TABLE %s ADD %s %s",tableName,columnName,columnType));
                 addColumnSqlList.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'",tableName,columnName,columnComment));
                break;
            case SQLSERVER:
            case HIVE:
                break;
            case MYSQL:
                addColumnSqlList.add("ALTER TABLE "+tableName+" ADD "+columnName+" "+columnType+" COMMENT '"+columnComment+"'");
                break;
            default : // mysql, sqlite
                
                break;
        }  
        
        return addColumnSqlList;
    }
            
    public static void resetOrganizationRepositoryDBConnection(int organizationId,int repositoryId)    
    {
        log.error("reset EntityManagerFactory! organizationId="+organizationId);
        String key = String.format("%d_%d",organizationId,repositoryId);
        dbConnMap.put(key, null);
    }
    
    public static void resetOrganizationRepositoryDBConnectionPool(int organizationId,int repositoryId)    
    {
        log.error("reset EntityManagerFactory! organizationId="+organizationId);
        String key = String.format("%d_%d",organizationId,repositoryId);
        dbConnPoolMap.put(key, null);
    }
     
    public static Connection getOrganizationRepositoryDBConnection(EntityManager em,EntityManager platformEm,int organizationId,int repositoryId) throws Exception
    {
        String key = String.format("%d_%d",organizationId,repositoryId);
        
        Connection conn = dbConnMap.get(key);
        
        if ( conn == null)
        {
            lockForDBConn.lock();// 取得锁

            conn = dbConnMap.get(key);
            
            if ( conn == null ) 
            {
                try 
                {
                    //platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
                    //em = Util.getEntityManagerFactory(organizationId).createEntityManager();

                    DatasourceConnection datasourceConnection = Util.getRepositorySQLdbDatasourceConnection(em,platformEm,repositoryId);
  
                    conn = JDBCUtil.getJdbcConnection(datasourceConnection);
                    conn.setAutoCommit(false);
                    dbConnMap.put(key,conn);
                }
                catch(Exception e)
                {  
                    log.error("failed to create connection ! e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e));
                    dbConnMap.put(key,conn);
                    throw e;
                }
                finally
                {
                    lockForDBConn.unlock(); 
                }
            }
            else
                lockForDBConn.unlock();
            
            return conn;
        }
        else
            return conn;
    }
    
    public static DataSource getOrganizationRepositoryDBConnectionPool(EntityManager em,EntityManager platformEm,int organizationId,int repositoryId) throws Exception
    {
        String key = String.format("%d_%d",organizationId,repositoryId);
        
        DataSource connPool = dbConnPoolMap.get(key);
        
        if ( connPool == null)
        {
            lockForDBConnPool.lock();// 取得锁

            connPool = dbConnPoolMap.get(key);
            
            if ( connPool == null ) 
            {
                try 
                {
                    DatasourceConnection datasourceConnection = Util.getRepositorySQLdbDatasourceConnection(em,platformEm,repositoryId);
                    connPool = JDBCUtil.getJdbcConnectionPool(datasourceConnection);
                    dbConnPoolMap.put(key,connPool);
                }
                catch(Exception e)
                {  
                    log.error("failed to create connection ! e="+e.getMessage()+" stacktrace="+ExceptionUtils.getStackTrace(e));
                    dbConnPoolMap.put(key,connPool);
                    throw e;
                }
                finally
                {
                    lockForDBConnPool.unlock(); 
                }
            }
            else
                lockForDBConnPool.unlock();
            
            return connPool;
        }
        else
            return connPool;
    }
        
    public static long getDatabaseTableCountNumber(String tableName, DatasourceConnection datasourceConnection) throws Exception
    {
        String sql = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Connection dbConn = null;
        String schema;
      
        int count = 0;
        
        try 
        {   
            schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
 
            if ( schema.equals("%") )
                schema = "";
            else
                schema = String.format("%s.",schema);
 
            sql = String.format("select count(*) from %s%s",schema,tableName);
 
            log.info(" countQuerySQL ="+sql);

            dbConn = getJdbcConnection(datasourceConnection);
            
            statement = dbConn.prepareStatement(sql);
            resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();

            count = resultSet.getInt(1);
        }
        catch(Exception e)
        {
            log.info("getQueryCount() failed! e="+e+" sql="+sql);
            return -1;
        }
        finally
        {           
            close(resultSet);
            close(statement);
            close(dbConn);
        }    

        return count;
    }
       
    public static long getDatabaseTableCountNumber(Datasource datasource, DatasourceConnection datasourceConnection) throws Exception
    {
        String sql = null;
        String filter = null;
        String databaseItem = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Connection dbConn = null;
        String schema;
      
        int count = 0;
        
        try 
        {  
            Map<String,String> map = getDatasourceConfig(datasource.getProperties());
                    
            filter = map.get("filter");
            databaseItem = map.get("databaseItem");
            
            schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
            //schema = map.get("schema");
            
            if ( schema.equals("%") )
                schema = "";
            else
                schema = String.format("%s.",schema);
            
            if ( filter == null || filter.trim().isEmpty() )
                sql = String.format("select count(*) from %s%s",schema,databaseItem);
            else
                sql = String.format("select count(*) from %s%s where %s",schema,databaseItem,filter);

            log.info(" countQuerySQL ="+sql);

            dbConn = getJdbcConnection(datasourceConnection);
            
            statement = dbConn.prepareStatement(sql);
            resultSet = JDBCUtil.executeQuery(statement);
            resultSet.next();

            count = resultSet.getInt(1);
        }
        catch(Exception e)
        {
            log.info("getQueryCount() failed! e="+e+" sql="+sql);
            return -1;
        }
        finally
        {
            close(resultSet);
            close(statement);
            close(dbConn);
        }    

        return count;
    }
 
    private static Map<String,String> getDatasourceConfig(String datasourceConfig) throws Exception
    {
        String databaseSelectionStr = null;
        Map<String,String> map = new HashMap<>();    
        
        try 
        {
            SAXReader saxReader = new SAXReader();
            ByteArrayInputStream in = new ByteArrayInputStream(datasourceConfig.getBytes("utf-8"));
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
  
            String databaseSelectionXml = String.format("<databaseSelection>%s</databaseSelection>", databaseSelectionStr);
            
            saxReader = new SAXReader();
            in = new ByteArrayInputStream(databaseSelectionXml.getBytes("utf-8"));
            Document doc = saxReader.read(in);            
            
            Element element = (Element)doc.selectSingleNode("//databaseSelection");           
            
            String databaseItem = element.element("databaseItemName").getTextTrim();
            String filter =element.element("filter").getTextTrim();
 
            map.put("databaseItem",databaseItem);
            map.put("filter",filter);
        }
        catch(Exception e) {
             throw e;
        }
        
        return map;
    }
       
    public static Connection getJdbcConnection(DatasourceConnection datasourceConnection) throws Exception
    {
        Connection conn = null;
        
        try
        {
            Map<String,String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());
            
            int databaseTypeId = Integer.parseInt(properties.get("database_type"));

            String schema =  properties.get("schema");
            if ( schema == null || schema.trim().isEmpty() )
                schema = "%";
                    
            String IPAddress = properties.get("ip_address");
            String port = properties.get("port");
            String databaseName = properties.get("database_name");
   
            String location = properties.get("location");
            
            if ( DatabaseType.findByValue(databaseTypeId) == DatabaseType.SQLITE )
                IPAddress = location;
            
            String sshHost = properties.get("ssh_host");
            
            Session session = null;
            Map<String,Object> sshTunnelMap = null;
            
            if ( sshHost != null && !sshHost.trim().isEmpty() )
            {
                String sshUser = properties.get("ssh_user");
                String sshPassword = properties.get("ssh_password");
                String sshPort = properties.get("ssh_port");
                String rHost = properties.get("remote_host");
                String rPort = properties.get("remote_port");
                String lPort = properties.get("local_port");
                
                sshTunnelMap = setupSSHTunnel(sshHost,sshUser,sshPassword,Integer.parseInt(sshPort),rHost,Integer.parseInt(rPort),Integer.parseInt(lPort));
            
                port = (String)sshTunnelMap.get("lPort"); log.info(" new port="+port);
            }
                                   
            conn = getJdbcConnection(databaseTypeId, IPAddress, port, databaseName, schema, datasourceConnection.getUserName(), datasourceConnection.getPassword());
        
            if ( sshTunnelMap != null )
            {
                session = (Session)sshTunnelMap.get("session");
                jschSessionMap.put(conn, session);
            }
        }
        catch(Exception e)
        {
            log.error(" getJdbcConnection() failed! e="+e+" datasourceConnectionId="+datasourceConnection.getId()+" stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
    
        return conn;
    }
    
    public static DataSource getJdbcConnectionPool(DatasourceConnection datasourceConnection) throws Exception
    {
        DataSource connPool = null;
        
        try
        {
            Map<String,String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());
            
            int databaseTypeId = Integer.parseInt(properties.get("database_type"));

            String schema =  properties.get("schema");
            if ( schema == null || schema.trim().isEmpty() )
                schema = "%";
                    
            String IPAddress = properties.get("ip_address");
            String port = properties.get("port");
            String databaseName = properties.get("database_name");
   
            String location = properties.get("location");
            
            if ( DatabaseType.findByValue(databaseTypeId) == DatabaseType.SQLITE )
                IPAddress = location;
            
            connPool = getJdbcConnectionPool(databaseTypeId, IPAddress, port, databaseName, schema, datasourceConnection.getUserName().trim(), datasourceConnection.getPassword().trim(),MAX_ACTIVE_CONNECTION);
        }
        catch(Exception e)
        {
            log.error(" getJdbcConnection() failed! e="+e+" datasourceConnectionId="+datasourceConnection.getId());
            throw e;
        }
    
        return connPool;
    }
     
    public static String getTableOrViewDescription(DatasourceConnection dcConn, String type,String tableName)
    {
        String schema;
        String driverName;
        Connection conn = null;
        String tableDescription = "";
 
        try
        {
            driverName = getDatabaseConnectionDriverName(dcConn); 
            schema = getDatabaseConnectionSchema(dcConn);
            
            Class.forName(driverName).newInstance();
            conn = getJdbcConnection(dcConn);
            
            DatabaseMetaData databaseMetaData = conn.getMetaData(); 
            ResultSet tableRet =databaseMetaData.getTables(null,schema,tableName,new String[]{type});
            tableRet.next();
            tableDescription = tableRet.getString("REMARKS");           
        }
        catch(Exception e)
        {
            log.error(" getTableOrVievDescription() failed! e="+e);
        }
        finally
        {
            close(conn);
        }  
        
        return tableDescription;
    }
        
    public static List<Map<String,Object>> getDatabaseTableOrViewColumnInfo(DatasourceConnection dcConn, String type,String itemName,String columnSchema)
    {
        List<Map<String,Object>> columnInfoList = new ArrayList<>();
        Map<String,Object> columnInfo;
        String schema;
        String driverName;
        Connection conn = null;
        ResultSet columnSet = null;
        ResultSet rs = null;
        Map<String,String> columnMap = new HashMap<>();

        try
        {
            driverName = getDatabaseConnectionDriverName(dcConn); 
            //schema = getDatabaseConnectionSchema(dcConn);
            //schema = "%";
            
            Class.forName(driverName).newInstance();
            
            Properties props = new Properties();
            props.put("user",dcConn.getUserName());
            props.put("password",dcConn.getPassword());
            props.put("remarksReporting","true");
            
            conn = getJdbcConnection(dcConn);
            
            DatabaseMetaData databaseMetaData = conn.getMetaData(); 
               
            //columnSet = databaseMetaData.getColumns(null, "%", itemName, "%");
            columnSet = databaseMetaData.getColumns(null, null, itemName, "%");

            while(columnSet.next())
            {
                columnInfo = new HashMap<>();
                
                columnInfo.put("isPrimaryKey", false);
                                 
                String tableSchema = columnSet.getString("TABLE_SCHEM");
                String columnName = columnSet.getString("COLUMN_NAME");
 
                //log.info("read column tableSchema="+tableSchema+" column scheam="+columnSchema+" columnName="+columnName);
                                                
               /* if ( (tableSchema == null || !tableSchema.toUpperCase().equals(columnSchema.toUpperCase())) && columnSchema!=null && !columnSchema.isEmpty() )
                    continue;*/
               
                if ( columnSchema!=null && !columnSchema.isEmpty() && !columnSchema.equals("null") )
                {
                    if ( tableSchema == null )
                        continue;
                    
                    if ( !tableSchema.toUpperCase().equals(columnSchema.toUpperCase()) )
                        continue;
                }
                      
                columnInfo.put("columnName",columnName);
                columnInfo.put("jdbcDataType", columnSet.getString("DATA_TYPE"));   
                columnInfo.put("typeName", columnSet.getString("TYPE_NAME"));
                
                String description = Tool.removeSpecificCharForNameAndDescription(columnSet.getString("REMARKS"));
                columnInfo.put("description", description);     
                
                columnInfo.put("length",columnSet.getString("COLUMN_SIZE")); 
                columnInfo.put("precision",columnSet.getString("DECIMAL_DIGITS"));
                columnInfoList.add(columnInfo);
            }
            
            Map<String,String> properties = Util.getDatasourceConnectionProperties(dcConn.getProperties());
           
            int databaseTypeId = Integer.parseInt(properties.get("database_type"));
    
            if ( DatabaseType.findByValue(databaseTypeId) != DatabaseType.PRESTO )
            {
                rs = databaseMetaData.getPrimaryKeys(null, null, itemName);
            
                while(rs.next()) 
                {
                    String primaryKeyColumnName = rs.getString("COLUMN_NAME");

                    for( Map<String,Object> map: columnInfoList)
                    {
                        if ( map.get("columnName").equals(primaryKeyColumnName))
                        {
                             map.put("isPrimaryKey", true);
                             break;
                        }
                    }
                }
            }      
        } catch (Exception e) {
            log.error(" getDatabaseTableOrViewColumnInfo() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        finally
        {
            close(columnSet);
            close(rs);
            close(conn);
        }  
 
        return columnInfoList;        
    }
    
    public static Map<String,Map<String,String>> getDatabaseTableOrViewColumnComment(Connection conn,String tableName)
    {
        Map<String,Map<String,String>> tableInfo = new HashMap<>();
        
        ResultSet columnSet = null;
    
        try
        {
            DatabaseMetaData databaseMetaData = conn.getMetaData(); 
   
            if ( tableName.contains(".") )
                tableName = tableName.substring(tableName.indexOf(".")+1);
            
            tableName = tableName.toLowerCase();
            
            columnSet = databaseMetaData.getColumns(null, null, tableName, "%");
 
            int k = 0;
            while(columnSet.next())
            {
                k++;
                Map<String,String> columnInfo = new HashMap<>();
                
                String columnName = columnSet.getString("COLUMN_NAME");
                
                String columnLen = columnSet.getString("COLUMN_SIZE"); 
                String columnComment = Tool.removeSpecificCharForNameAndDescription(columnSet.getString("REMARKS"));                
                
                columnInfo.put("columnLen",columnLen);
                columnInfo.put("columnComment",columnComment);
                
                tableInfo.put(columnName.toLowerCase(),columnInfo);
            }
            
            if ( k == 0 )
            {
                tableName = tableName.toUpperCase();
            
                columnSet = databaseMetaData.getColumns(null, null, tableName, "%");

                while(columnSet.next())
                {
                    Map<String,String> columnInfo = new HashMap<>();

                    String columnName = columnSet.getString("COLUMN_NAME");

                    String columnLen = columnSet.getString("COLUMN_SIZE"); 
                    String columnComment = Tool.removeSpecificCharForNameAndDescription(columnSet.getString("REMARKS"));                

                    columnInfo.put("columnLen",columnLen);
                    columnInfo.put("columnComment",columnComment);

                    tableInfo.put(columnName.toLowerCase(),columnInfo);
                }
            }
        } 
        catch (Exception e) {
            log.error(" getDatabaseTableOrViewColumnComment() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
     
        return tableInfo;        
    }
    
    public static List<String> getDatabaseTableOrViewColumnSchemaList(DatasourceConnection dcConn, String type,String itemName)
    {
        List<String> columnSchemaList = new ArrayList<>();
        String schema;
        String driverName;
        Connection conn = null;
        ResultSet columnSet = null;
        ResultSet rs = null;
 
        try
        {
            driverName = getDatabaseConnectionDriverName(dcConn); 
            schema = getDatabaseConnectionSchema(dcConn);
 
            Class.forName(driverName).newInstance();
            
            Properties props = new Properties();
            props.put("user",dcConn.getUserName());
            props.put("password",dcConn.getPassword());
            props.put("remarksReporting","true");
            
            conn = getJdbcConnection(dcConn);
            
            DatabaseMetaData databaseMetaData = conn.getMetaData(); 
               
            columnSet = databaseMetaData.getColumns(null, "%", itemName, "%");

            log.info("2222222222222222 check databaseitme ="+itemName);
            
            while(columnSet.next())
            {
                String tableSchema = columnSet.getString("TABLE_SCHEM");
              
                log.info("333333333333 tableschem="+tableSchema);
                
                if ( tableSchema == null )
                    continue;
                
                if ( Tool.ifListContainsStr(columnSchemaList,tableSchema) == false )
                {
                    columnSchemaList.add(tableSchema);
                    log.info(" add new tableSchema "+tableSchema);
                }
            }            
        } catch (Exception e) 
        {
            log.error(" getDatabaseTableOrViewColumnSchemaList() failed! e="+e+ " stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        finally
        {
            close(columnSet);
            close(rs);
            close(conn);
        }  
 
        return columnSchemaList;        
    }
    
    public static List<Map<String,Object>> getHiveDatabaseTableOrViewColumnInfo(DatasourceConnection dcConn, String type,String itemName) throws Exception
    {
        List<Map<String,Object>> columnInfoList = new ArrayList<>();
        Map<String,Object> columnInfo;
        String schema;
        String driverName;
        Connection conn = null;
        ResultSet columnSet = null;
        ResultSet rs = null;

        try
        {
            driverName = getDatabaseConnectionDriverName(dcConn); 
            schema = getDatabaseConnectionSchema(dcConn);
            
            Class.forName(driverName).newInstance();
            
            Properties props = new Properties();
            props.put("user",dcConn.getUserName());
            props.put("password",dcConn.getPassword());
            props.put("remarksReporting","true");

            conn = getJdbcConnection(dcConn);
            Statement stmt = conn.createStatement();
         
            rs = stmt.executeQuery("describe "+itemName);
            
            while (rs.next()) 
            {
                columnInfo = new HashMap<>();
                columnInfo.put("isPrimaryKey", false);
                columnInfo.put("columnName", rs.getString("col_name"));
                
                String dataTypeStr = rs.getString("data_type"); 
                columnInfo.put("jdbcDataType", String.valueOf(Util.convertStringTypeToJdbcType(dataTypeStr)) );
                
                columnInfo.put("typeName", rs.getString("data_type"));                 
                columnInfo.put("description", rs.getString("comment")); 
 
                columnInfoList.add(columnInfo);
            }
        } 
        catch (Exception e) {
            log.info(" getSparkSQLDatabaseTableOrViewColumnInfo() failed! e="+e);
            throw e;
        }
        finally
        {
            close(columnSet);
            close(rs);
            close(conn);
        }  
 
        return columnInfoList;        
    }
    
    public static List<String> getOracleSynonymTableName(DatasourceConnection dcConn, String synonymName,String schema)
    {
        String tableInfo;
        String driverName;
        Connection conn = null;
        ResultSet columnSet = null;
        ResultSet rs = null;
        List<String> tableInfoList = new ArrayList<>();

        try
        {
            driverName = getDatabaseConnectionDriverName(dcConn); 
      
            Class.forName(driverName).newInstance();
            
            Properties props = new Properties();
            props.put("user",dcConn.getUserName());
            props.put("password",dcConn.getPassword());
            props.put("remarksReporting","true");

            conn = getJdbcConnection(dcConn);
            Statement stmt = conn.createStatement();
         
            //String sql = String.format("select table_name,table_owner from dba_synonyms WHERE synonym_name='%s' and owner='%s'",synonymName,schema.toUpperCase());
            String sql = String.format("select table_name,table_owner from dba_synonyms WHERE synonym_name='%s'",synonymName);
          
            log.info("444444444444444444444 sql="+sql);
            rs = stmt.executeQuery(sql);
            
            while (rs.next()) 
            {
                tableInfo = rs.getString("table_owner")+","+rs.getString("table_name"); log.info(" 7777 get tableInfo ="+tableInfo);
                tableInfoList.add(tableInfo);
            }
        } 
        catch (Exception e) {
            log.info(" getOracleSynonymTableName() failed! e="+e+"synonymName="+synonymName+" schema="+schema);
        }
        finally
        {
            close(columnSet);
            close(rs);
            close(conn);
        }  
 
        return tableInfoList;        
    }
    
    public static List<String> getDatabaseTableOrViewList(DatasourceConnection datasourceConnection, String type)
    {
        String tableOrViewName;
        String tableSchema = null;
        List<String> list = new ArrayList<>();

        String schema;
        Connection conn = null;
        ResultSet tableSet = null;

        try
        {            
            schema = getDatabaseConnectionSchema(datasourceConnection);
            //schema = "%";
            
            if ( schema.equals("%") )
                schema = null;
     
            conn = getJdbcConnection(datasourceConnection);         
            
            DatabaseMetaData databaseMetaData = conn.getMetaData();
        
            log.info(" 1111111111111 schema = "+schema);
            
            DatabaseType databaseType = getDatabaseConnectionDatabaseType(datasourceConnection);
            
            tableSet = databaseMetaData.getTables(null, schema, "%", new String[]{type});
           
            while(tableSet.next())
            {
                tableSchema = tableSet.getString("TABLE_SCHEM");
                //log.info("2222222222 tableSchema="+tableSchema);
                      
                if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) && schema == null )
                {
                    if ( !tableSchema.toUpperCase().equals(datasourceConnection.getUserName().trim().toUpperCase()) )
                        continue;
                }
              
                if ( schema != null )
                {
                    if ( (tableSchema == null || !tableSchema.toUpperCase().equals(schema.toUpperCase())) )
                        continue;
                }

                tableOrViewName = tableSet.getString("TABLE_NAME");     
                
                if ( tableOrViewName.contains("$") || tableOrViewName.contains(".") )
                    continue;
                
                list.add(tableOrViewName);
               
            }
        } 
        catch (Exception e) {
            log.error(" getDatabaseTableOrViewList() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        finally
        {
            close(tableSet);
            close(conn);
        }  

        return list;
    }
    
    public static List<String> getDatabaseTableOrViewList(Connection dbConn, DatabaseType databaseType, String schema, String type,String user)
    {
        String tableOrViewName;
        String tableSchema = null;
        List<String> list = new ArrayList<>();
 
        ResultSet tableSet = null;

        try
        {                        
            if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
            {
                if ( schema == null || schema.trim().isEmpty() )
                    schema = user;
                
                Statement stmt = dbConn.createStatement();
            
                try
                {
                    String sql = String.format("select OBJECT_NAME from dba_objects a where (a.owner='%s' OR a.owner='%s' ) AND Object_type in ('%s')",schema,schema.toUpperCase(),type);
                    log.info("111111111 sql="+sql);

                    ResultSet rs=stmt.executeQuery(sql);
                    while (rs.next())
                        list.add(rs.getString(1));
                }
                catch(Exception ee)
                {
                    log.warn(" read dba_objects failed,try all_dataobjects! ee="+ee);

                    String sql = String.format("select OBJECT_NAME from all_objects a where (a.owner='%s' OR a.owner='%s' ) AND Object_type in ('%s')",schema,schema.toUpperCase(),type);
                    log.info("2222222 sql="+sql);

                    ResultSet rs=stmt.executeQuery(sql);
                    while (rs.next())
                        list.add(rs.getString(1));
                }
            }
            else
            {
                DatabaseMetaData databaseMetaData = dbConn.getMetaData();
        
                log.info(" 1111111111111 schema = ["+schema+"]");
                
                if ( schema.trim().isEmpty() )
                    schema = null;

                tableSet = databaseMetaData.getTables(null, schema, "%", new String[]{type});

                while(tableSet.next())
                {
                    tableSchema = tableSet.getString("TABLE_SCHEM");
         
                    if ( (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) && schema == null )
                    {
                        if ( !tableSchema.toUpperCase().equals(user.toUpperCase()) )
                            continue;
                    }
              
                    if ( schema != null )
                    {
                        if ( (tableSchema == null || !tableSchema.toUpperCase().equals(schema.toUpperCase())) )
                            continue;
                    }

                    tableOrViewName = tableSet.getString("TABLE_NAME");     

                    if ( tableOrViewName.contains("$") || tableOrViewName.contains(".") )
                        continue;

                    list.add(tableOrViewName);
                }
            }
            
        } 
        catch (Exception e) {
            log.error(" getDatabaseTableOrViewList() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        finally
        {
            close(tableSet);
        }  

        return list;
    }
          
    public static List<String> getSparkSQLDatabaseTableOrViewList(DatasourceConnection datasourceConnection, String type)
    {
        List<String> list = new ArrayList<>();
 
        Connection conn = null;
        ResultSet tableSet = null;
        String sql;

        try
        {            
            conn = getJdbcConnection(datasourceConnection);                    
            Statement stmt = conn.createStatement();
            
            if ( type.equals("TABLE") )
                sql = "show tables";
            else
                return list;
      
            ResultSet rs=stmt.executeQuery(sql);
            while (rs.next())
                list.add(rs.getString(1));
        
        } catch (Exception e) {
            log.error("getSparkSQLDatabaseTableOrViewList() failed! e="+e);
        }
        finally
        {
            close(tableSet);
            close(conn);
        }  

        return list;
    }
    
     public static List<String> getOracleDatabaseTableOrViewList(DatasourceConnection datasourceConnection,String schema, String type)
    {
        List<String> list = new ArrayList<>();
 
        Connection conn = null;
        ResultSet tableSet = null;

        try
        {            
            conn = getJdbcConnection(datasourceConnection);                    
            Statement stmt = conn.createStatement();
            
            try
            {
                String sql = String.format("select OBJECT_NAME from dba_objects a where (a.owner='%s' OR a.owner='%s' ) AND Object_type in ('%s')",schema,schema.toUpperCase(),type);
                log.info("111111111 sql="+sql);

                ResultSet rs=stmt.executeQuery(sql);
                while (rs.next())
                    list.add(rs.getString(1));
            }
            catch(Exception ee)
            {
                log.warn(" read dba_objects failed,try all_dataobjects! ee="+ee);
                
                 String sql = String.format("select OBJECT_NAME from all_objects a where (a.owner='%s' OR a.owner='%s' ) AND Object_type in ('%s')",schema,schema.toUpperCase(),type);
                log.info("2222222 sql="+sql);

                ResultSet rs=stmt.executeQuery(sql);
                while (rs.next())
                    list.add(rs.getString(1));
            }
        
        } catch (Exception e) {
            log.error("getOracleDatabaseTableOrViewList() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        finally
        {
            close(tableSet);
            close(conn);
        }  

        return list;
    }
             
             
    public static boolean isTableExists(Connection conn, String schema, String tableName) throws Exception
    {
        try
        {   
            DatabaseMetaData databaseMetaData = conn.getMetaData();
        
            ResultSet tableSet = databaseMetaData.getTables(null, schema, "%", new String[]{"TABLE"});

            while(tableSet.next())
            {
                String tName = tableSet.getString("TABLE_NAME");
                
                if ( tableName.toLowerCase().equals(tName.toLowerCase()) )
                    return true;
            }
            
            return false;
        } 
        catch (Exception e) {
            throw e;
        }
    }
        
    public static void createTable(Connection conn, String createTableSql) throws Exception
    {
        Statement stmt = null;

        try
        {           
            stmt = conn.createStatement();
            stmt.executeUpdate(createTableSql);
        } 
        catch (Exception e) {
            throw e;
        }
    }
         
    public static String getDatabaseConnectionDriverName(DatasourceConnection dcConn) 
    {
        String databaseType = "";
        String driverName = "";
        
        Document propertiesXml = Tool.getXmlDocument(dcConn.getProperties());
        
        List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_CONNECTION_PROPERTIES);
        
        for(Element element: list)
        {
            Element nameElement = element.element("name");
            
            if ( nameElement.getTextTrim().equals("database_type"))
            {
                databaseType = element.element("value").getTextTrim();
                driverName = DatabaseType.findByValue(Integer.parseInt(databaseType)).getDriver();
                break;
            }
        }

        return driverName;
    } 
    
    public static DatabaseType getDatabaseConnectionDatabaseType(DatasourceConnection dcConn) 
    {
        String databaseType;
        
        Document propertiesXml = Tool.getXmlDocument(dcConn.getProperties());
        
        List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_CONNECTION_PROPERTIES);
        
        for(Element element: list)
        {
            Element nameElement = element.element("name");
            
            if ( nameElement.getTextTrim().equals("database_type"))
            {
                databaseType = element.element("value").getTextTrim();
                return DatabaseType.findByValue(Integer.parseInt(databaseType));
            }
        }

        return null;
    }

    public static String getDatabaseConnectionSchema(DatasourceConnection dcConn) 
    {
        String schema = null;
        
        Document propertiesXml = Tool.getXmlDocument(dcConn.getProperties());
        
        List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_CONNECTION_PROPERTIES);
        
        for(Element element: list)
        {
            Element nameElement = element.element("name");
            
            if ( nameElement.getTextTrim().equals("schema"))
            {
                schema = element.element("value").getTextTrim();
                break;
            }
        }

        if ( schema == null || schema.trim().isEmpty() )
            schema = "%";
        
        return schema;
    }
    
    public static DataSource getJdbcConnectionPool(int databaseTypeId, String IPAddress, String port, String databaseName, String schema, String user, String password, int maxActive) throws Exception
    {
        String driverName;
        String url;
        String urlTemplate;
        String defaultPort;
        
        BasicDataSource ds = new BasicDataSource();
        
        driverName = DatabaseType.findByValue(databaseTypeId).getDriver();
        ds.setDriverClassName(driverName);
        
        urlTemplate = DatabaseType.findByValue(databaseTypeId).getUrlTemplate();
        defaultPort = DatabaseType.findByValue(databaseTypeId).getDefaultPort();
            
        if ( DatabaseType.findByValue(databaseTypeId) == DatabaseType.SQLITE )
        {
            url = String.format(urlTemplate,IPAddress);
        }
        else
        {
            if ( port == null || port.trim().isEmpty() )
                url = String.format(urlTemplate,IPAddress,defaultPort,databaseName);
            else
                url = String.format(urlTemplate,IPAddress,port,databaseName);
        }
        
        ds.setUrl(url);
        
        ds.setUsername(user);
        ds.setPassword(password);
        
        ds.setMaxActive(maxActive);
        ds.setMinIdle(2);
        ds.setMaxIdle(10);
             
        return ds;
    }    
    
    public static Connection getJdbcConnection(int databaseTypeId, String IPAddress, String port, String databaseName, String schema, String user, String password) throws Exception
    {
        String driverName;
        Connection conn;
        String url = null;
        String urlTemplate;
        String defaultPort;
        DatabaseType databaseType;
        Driver driver = null;
        
        try
        {
            databaseType = DatabaseType.findByValue(databaseTypeId);
            
         /*   if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE ||
                    databaseType == DatabaseType.ORACLE_8i )
            {                 
                URL jarUrl = new URL("jar","","file:"+libFilePath+databaseType.getDriverPath()+"!/"); 
                log.info(" jarUrl="+jarUrl);
        
                URLClassLoader classLoader = new URLClassLoader(new URL[]{jarUrl}); 
                Class driverClass = classLoader.loadClass(databaseType.getDriver());
                driver = (Driver)driverClass.newInstance();
                DriverManager.registerDriver(driver);
            }
            else
            { */
                driverName = databaseType.getDriver();
                Class.forName(driverName).newInstance();
         //   }
            
            urlTemplate = databaseType.getUrlTemplate();
            defaultPort = databaseType.getDefaultPort();
            
            if ( databaseType == DatabaseType.SQLITE )
            {
                url = String.format(urlTemplate,IPAddress);
            }
            else
            {
                if ( databaseName == null || databaseName.trim().isEmpty() )
                    databaseName = "";
                
                if ( port == null || port.trim().isEmpty() )
                    url = String.format(urlTemplate,IPAddress,defaultPort,databaseName);
                else
                    url = String.format(urlTemplate,IPAddress,port,databaseName);
            }
            
            Properties props = new Properties();
            
            if ( user != null && !user.isEmpty() )
                props.put("user",user);
            
            if ( password != null && !password.isEmpty() )
                props.put("password",password);
            
            props.put("remarksReporting","true");
            props.put("useInformationSchema","true");
       
            log.info(" url="+url);
            
            if ( databaseType == DatabaseType.PRESTO )
                conn = DriverManager.getConnection(url,user,null); 
            else
                conn = DriverManager.getConnection(url,props); 
             
         /*   if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE ||
                    databaseType == DatabaseType.ORACLE_8i )
            {
                oracleDBConn.put(conn, driver);
            }*/
                
            return conn;
        }
        catch(Exception e)
        {
            log.error("Failed: ["+e+"] url="+url+" databaseTypeId="+databaseTypeId+" ip="+IPAddress+" databaseName="+databaseName+" user="+user+" password="+password);            

            throw e;
        }
    }
      
    public static String testJdbcConnection(int databaseTypeId, String IPAddress, String port, String databaseName, String schema, String user, String password)
    {
        String testInfo = "Failed!";
        String driverName;
        Connection conn = null;
        String url = null;
        String urlTemplate;
        String defaultPort;
        
        try
        {
            driverName = DatabaseType.findByValue(databaseTypeId).getDriver();
            Class.forName(driverName).newInstance();
            
            urlTemplate = DatabaseType.findByValue(databaseTypeId).getUrlTemplate();
            defaultPort = DatabaseType.findByValue(databaseTypeId).getDefaultPort();
            
            if ( DatabaseType.findByValue(databaseTypeId) == DatabaseType.SQLITE )
            {
                url = String.format(urlTemplate,IPAddress);
            }
            else
            {
                if ( port == null || port.trim().isEmpty() )
                    url = String.format(urlTemplate,IPAddress,defaultPort,databaseName);
                else
                    url = String.format(urlTemplate,IPAddress,port,databaseName);
            }
     
            if ( password == null )
                password = "";
            
            conn = DriverManager.getConnection(url, user, password.trim()); 
            
            if ( conn != null )
                testInfo = "OK!";
        }
        catch(Exception e)
        {
            String errorInfo = "Failed: "+e+" databaseTypeId="+databaseTypeId+" ip="+IPAddress+" databaseName="+databaseName+" user="+user+" password="+password+" stacktrace="+ExceptionUtils.getStackTrace(e);            
            testInfo = "Failed: url="+url+" errorInfo = ["+errorInfo+"]";
        }
        finally
        {
            if ( conn != null )
                close(conn);
        }
    
        return testInfo;
    }
    
    public static Map<String,Object> setupSSHTunnel(String sshHost,String sshUser,String sshPassword,int sshPort,String rHost,int rPort,int lPort) throws Exception 
    {   
        Map<String,Object> map = new HashMap<>();
        
        try 
        {  
            log.info(" sshHost="+sshHost+", sshUser="+sshUser+", sshPassword="+sshPassword+", sshPort="+sshPort+", rHost="+rHost+", rPort="+rPort+",lPort="+lPort);
            
            JSch jsch = new JSch();  
            Session session = jsch.getSession(sshUser, sshHost, sshPort);  
            session.setPassword(sshPassword);  
            session.setConfig("StrictHostKeyChecking", "no");  
            session.connect();
            log.info(" ssh server version="+session.getServerVersion());//这里打印SSH服务器版本信息  
            
            int retry = 0;
            
            while(true)
            {
                retry++;
                
                try
                {
                  int assinged_port = session.setPortForwardingL(lPort, rHost, rPort);  
                  log.info("localhost:" + assinged_port + " -> " + rHost + ":" + rPort);
                  break;
                }
                catch(Exception e)
                {
                    log.error("bing port["+lPort+"] failed! increase it!");
                    lPort++; 
                    
                    if ( retry > 200 )
                        throw e;
                }
            }
          
            map.put("lPort", String.valueOf(lPort));
            map.put("session",session);
            
            return map;
        } 
        catch (Exception e) 
        {  
            log.info(" setupSSHTunnel() failed! e="+e);
            throw e;
        }
    }  
     
    public static Connection getConnection(String driver,String dbURL, String user, String password) throws Exception
    {
        try 
        {
            Class.forName(driver).newInstance();
            Connection conn = DriverManager.getConnection(dbURL, user, password);
            
            return conn;
        } 
        catch (Exception e) 
        {
            throw e;
        }
    }

    public static boolean executeUpdate(Statement statement,String sql) 
    {
        try 
        {
          statement.executeUpdate(sql);
          return true;
        } catch (SQLException e) {
          return false;
        }
    }

    public static ResultSet executeQuery(PreparedStatement statement) throws SQLException 
    {
        ResultSet rs = null;
    
        try {
            rs = statement.executeQuery();
        } catch (SQLException e) {
            throw e;
        }
        return rs;
    }
    
    public static void close(ResultSet resultSet) 
    {
        try 
        {
            if ( resultSet != null )
                resultSet.close();
        } 
        catch (Throwable t) {
            log.error(" close resultset failed! t="+t);
        }
    }
    
    public static void close(Statement statement) 
    {
        try 
        {
            if ( statement != null )
                statement.close();
        } 
        catch (Throwable t) {
            log.error(" close statement failed! t="+t);
        }
    }

    public static void close(Connection conn) 
    {
        try 
        {
            if ( conn != null )
                conn.close();
 
            Session session = jschSessionMap.get(conn);
            if ( session != null )
                session.disconnect();
            
        /*    Driver driver = oracleDBConn.get(conn);
            
            if ( driver != null)
            {
                oracleDBConn.remove(conn);
                DriverManager.deregisterDriver(driver);
            }*/
        } 
        catch (Throwable t) {
              log.error(" close conn failed! t="+t);
        }
    }   
    
    public static byte[] getClobString(Clob b) 
    {
        if (b != null) 
        {
            try 
            {
                InputStream in = b.getAsciiStream();
                byte[] by = new byte[(int) b.length()];
                in.read(by, 0, (int) b.length());
                in.close();

                return by;
            } catch (Exception e) {
                    // TODO Auto-generated catch block
                    System.out.println(e.getMessage());
            }

        }
        return new byte[0];
    }

    public static byte[] getBlobString(Blob b) 
    {
        if (b != null) 
        {
            try 
            {
                InputStream in = b.getBinaryStream();
                byte[] by = new byte[(int) b.length()];
                in.read(by, 0, (int) b.length());
                in.close();

                return by;
            } catch (Exception e) {
                    // TODO Auto-generated catch block
                    System.out.println(e.getMessage());
            }
        }
        return new byte[0];
    }

    public static boolean isGPReservedWord(String columnName)
    {
        if ( columnName == null || columnName.trim().isEmpty() )
            return false;
        
        String gpReservedWords = "offset,limit";
        
        if ( gpReservedWords.contains(columnName.trim().toLowerCase()) )
            return true;
        else
            return false;
    }
     
    public static String getSqlWithPage(DatabaseType databaseType,long startLine,int pageSize,String sql) // 1,1000; 1001,1000
    {
        String newSql = sql;
 
        if ( databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE )
        {
            newSql = String.format("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM ( %s ) A WHERE ROWNUM <= %d ) WHERE RN >= %d",sql,startLine+pageSize-1, startLine);
        }
        else
        if ( databaseType == DatabaseType.MYSQL )
        {
            newSql = String.format("%s limit %d,%d",sql,startLine-1,pageSize);
        }
        else
        if ( databaseType == DatabaseType.GREENPLUM )
        {
            newSql = String.format("%s offset %d limit %d",sql,startLine-1,pageSize);
        }
 
        return newSql;
    }
    
}

class SimpleSQLSturecture
{
    List<String> selectFieldNames = new ArrayList<>();
    String familiesStr;
    String tableName;
    Map<String,Map<Object,String>> where = new HashMap<>();
    long startFrom = 0;
    long maxExpected = Long.MAX_VALUE;
    String startRow;
    String stopRow;
    long maxExpectedFromClient;

    public SimpleSQLSturecture() {
    }

    public List<String> getSelectFieldNames() {
        return selectFieldNames;
    }

    public void setSelectFieldNames(List<String> selectFieldNames) {
        
        List<String> newList = new ArrayList<>();
        
        for(String name : selectFieldNames)
        {
            newList.add(name.trim());
        }
        
        this.selectFieldNames = newList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Map<Object, String>> getWhere() {
        return where;
    }

    public void setWhere(Map<String, Map<Object, String>> where) {
        this.where = where;
    }    

    public String getFamiliesStr() {
        return familiesStr;
    }

    public void setFamiliesStr(String familiesStr) {
        this.familiesStr = familiesStr;
    }

    public long getStartFrom() {
        return startFrom;
    }

    public void setStartFrom(long startFrom) {
        this.startFrom = startFrom;
    }

    public long getMaxExpected() {
        return maxExpected;
    }

    public void setMaxExpected(long maxExpected) {
        this.maxExpected = maxExpected;
    }

    public String getStartRow() {
        return startRow;
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }

    public String getStopRow() {
        return stopRow;
    }

    public void setStopRow(String stopRow) {
        this.stopRow = stopRow;
    }

    public long getMaxExpectedFromClient() {
        return maxExpectedFromClient;
    }

    public void setMaxExpectedFromClient(long maxExpectedFromClient) {
        this.maxExpectedFromClient = maxExpectedFromClient;
    }
    
    
}